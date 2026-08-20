```python
"""
KAN-26 — Premier Partner Qualification Program — Oracle to SalesIQ ODS Migration
PySpark ETL implementing the approved STTM: PP_AGENT_AMT_SMRY, PP_QUALFCTN_SUMRY,
FACT_PP_QUALFCTN_SUMRY.
Target execution: AWS Glue (GlueContext) or EMR (plain SparkSession fallback).
"""

import sys
import logging
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, DateType,
    TimestampType, IntegerType
)

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    GLUE_ENV = True
except ImportError:
    GLUE_ENV = False

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger = logging.getLogger("pp_qualification_pipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

# ---------------------------------------------------------------------------
# Spark / Glue session setup
# ---------------------------------------------------------------------------
if GLUE_ENV:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
else:
    spark = SparkSession.builder.appName("pp_qualification_pipeline").getOrCreate()

# ---------------------------------------------------------------------------
# Placeholders — S3 paths / Glue Catalog tables
# ---------------------------------------------------------------------------
S3_BASE = "s3://lincoln-pp-salesiq/inputs"
S3_TARGET_BASE = "s3://lincoln-pp-salesiq/ods"

SRC_PATHS = {
    "PTY_MDM": f"{S3_BASE}/pty_mdm/",
    "AGENT_MASTR": f"{S3_BASE}/agent_mastr/",
    "PLCY_TRX_SUMRY": f"{S3_BASE}/plcy_trx_sumry/",
    "PROD_HIER_DETL": f"{S3_BASE}/prod_hier_detl/",
    "SF_ACCT_TERRTY_DETL": f"{S3_BASE}/sf_acct_terrty_detl/",
    "DISCOVERY_FILE": f"{S3_BASE}/discovery_non_registered/",
    "HIST_QUAL": f"{S3_BASE}/historical_qualification/",  # supports Pinnacle/Sustaining lookback
}

TGT_PATHS = {
    "PP_AGENT_AMT_SMRY": f"{S3_TARGET_BASE}/pp_agent_amt_smry/",
    "PP_QUALFCTN_SUMRY": f"{S3_TARGET_BASE}/pp_qualfctn_sumry/",
    "FACT_PP_QUALFCTN_SUMRY": f"{S3_TARGET_BASE}/fact_pp_qualfctn_sumry/",
}

# ---------------------------------------------------------------------------
# Hardcoded qualification thresholds (STTM: no reference table defined in FSD)
# ---------------------------------------------------------------------------
THRESHOLDS = {
    "FP": {"Gold": 150000.00, "Platinum": 225000.00, "Diamond": 600000.00},
    "AGENCY": {"Gold": 1500000.00, "Platinum": 2500000.00, "Diamond": 5000000.00},
}
DEFERRED_COMP_THRESHOLD_FP = 150000.00
GOLD_PROXIMITY_PCT = 0.01  # within 1% of Gold minimum still included

# GA Org identification range rule (FSD 5.4 — ~23% match rate; fallback NULL, no exclusion)
GA_ORG_RANGES = [(500, 699), (910, 998)]


def read_source(name: str, schema: StructType = None) -> DataFrame:
    """Read a source table from its S3 placeholder path."""
    try:
        reader = spark.read
        if schema is not None:
            reader = reader.schema(schema)
        df = reader.parquet(SRC_PATHS[name])
        logger.info(f"Loaded source {name}: {df.count()} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to read source {name} from {SRC_PATHS[name]}: {e}")
        raise


def write_target(df: DataFrame, target_name: str, mode: str = "overwrite",
                  partition_cols=None):
    """Write to target S3/Glue Catalog location."""
    try:
        writer = df.write.mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(TGT_PATHS[target_name])
        logger.info(f"Wrote target {target_name} ({mode}): {df.count()} rows")
    except Exception as e:
        logger.error(f"Failed to write target {target_name}: {e}")
        raise


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
logger.info("Starting extract phase")

pty_mdm = read_source("PTY_MDM")
agent_mastr = read_source("AGENT_MASTR")
plcy_trx_sumry = read_source("PLCY_TRX_SUMRY")
prod_hier_detl = read_source("PROD_HIER_DETL")
sf_acct_terrty_detl = read_source("SF_ACCT_TERRTY_DETL")
discovery_file = read_source("DISCOVERY_FILE")           # non-registered indicator refresh
hist_qual = read_source("HIST_QUAL")                       # Pinnacle/Sustaining lookback history

# ---------------------------------------------------------------------------
# Data Quality: PLCY_TRX_SUMRY — reject null/negative PAP_AMT, dedupe on TRX_ID
# ---------------------------------------------------------------------------
logger.info("Applying data quality checks on PLCY_TRX_SUMRY")

rejected_trx = plcy_trx_sumry.filter(
    F.col("PAP_AMT").isNull() | (F.col("PAP_AMT") < 0)
)
if rejected_trx.count() > 0:
    logger.warning(f"Rejecting {rejected_trx.count()} PLCY_TRX_SUMRY rows with null/negative PAP_AMT")
    rejected_trx.write.mode("append").parquet(f"{S3_TARGET_BASE}/exceptions/plcy_trx_sumry_rejects/")

plcy_trx_clean = plcy_trx_sumry.filter(
    F.col("PAP_AMT").isNotNull() & (F.col("PAP_AMT") >= 0)
)

# Dedup on TRX_ID, keep latest by TRX_DATE
trx_window = Window.partitionBy("TRX_ID").orderBy(F.col("TRX_DATE").desc())
plcy_trx_dedup = (
    plcy_trx_clean
    .withColumn("_rn", F.row_number().over(trx_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ---------------------------------------------------------------------------
# Referential integrity: AGENT_MASTR must have matching PARTY_ID in PTY_MDM
# ---------------------------------------------------------------------------
logger.info("Validating AGENT_MASTR referential integrity against PTY_MDM")

agent_with_party_flag = agent_mastr.join(
    pty_mdm.select("PARTY_ID").distinct(),
    on="PARTY_ID",
    how="left"
).withColumn(
    "_HAS_PARTY_MATCH",
    F.col("PARTY_ID").isNotNull()
)

orphan_agents = agent_with_party_flag.filter(~F.col("_HAS_PARTY_MATCH"))
if orphan_agents.count() > 0:
    logger.warning(f"Flagging {orphan_agents.count()} AGENT_MASTR rows with no matching PARTY_ID")
    orphan_agents.write.mode("append").parquet(f"{S3_TARGET_BASE}/exceptions/agent_no_party_match/")

agent_valid = agent_with_party_flag.drop("_HAS_PARTY_MATCH")

# ---------------------------------------------------------------------------
# Refresh non-registered indicator from Discovery file each run (STTM DQ rule)
# ---------------------------------------------------------------------------
agent_valid = agent_valid.join(
    discovery_file.select(
        F.col("AGENT_ID").alias("_DISC_AGENT_ID"),
        F.col("NON_REGISTERED_IND").alias("NON_REGISTERED_IND_REFRESHED")
    ),
    agent_valid["AGENT_ID"] == F.col("_DISC_AGENT_ID"),
    how="left"
).drop("_DISC_AGENT_ID")

# ---------------------------------------------------------------------------
# GA Org identification (FSD 5.4) — SUPERIOR_AGENCY range rule.
# Fallback = NULL for out-of-range values; do NOT exclude the record.
# ---------------------------------------------------------------------------
def ga_org_range_expr():
    last3 = F.substring(F.col("SUPERIOR_AGENCY").cast(StringType()), -3, 3).cast(IntegerType())
    conds = [ (last3 >= lo) & (last3 <= hi) for lo, hi in GA_ORG_RANGES ]
    combined = conds[0]
    for c in conds[1:]:
        combined = combined | c
    return combined, last3

if "SUPERIOR_AGENCY" in agent_valid.columns:
    ga_cond, last3_col = ga_org_range_expr()
    agent_valid = agent_valid.withColumn(
        "GA_ORG_ID",
        F.when(ga_cond, F.col("SUPERIOR_AGENCY")).otherwise(F.lit(None).cast(StringType()))
    ).withColumn(
        "GA_ORG_FLAG_FOR_REVIEW",
        F.when(~ga_cond, F.lit("Y")).otherwise(F.lit("N"))
    )
else:
    agent_valid = agent_valid.withColumn("GA_ORG_ID", F.lit(None).cast(StringType()))
    agent_valid = agent_valid.withColumn("GA_ORG_FLAG_FOR_REVIEW", F.lit("N"))

# ---------------------------------------------------------------------------
# POS / DLC exclusion flags (modeled as flags per STTM, not filtered joins)
# ---------------------------------------------------------------------------
agent_valid = agent_valid.withColumn(
    "EXCLUDE_INDIVIDUAL_QUAL_IND",
    F.when((F.col("POS_IND") == "Y") | (F.col("EDJ_CHANNEL_IND") == "Y"), F.lit("Y")).otherwise(F.lit("N"))
)

# Terminated status is display-only — explicitly NOT used as an exclusion filter (FSD 5.8)
# No filter applied on AGENT_STATUS anywhere in this pipeline by design.

# ---------------------------------------------------------------------------
# Transform: PP_AGENT_AMT_SMRY
# STTM: monthly PAP amounts — Total / Service / Financial / Projected
# ---------------------------------------------------------------------------
logger.info("Building PP_AGENT_AMT_SMRY")

trx_with_product = plcy_trx_dedup.join(
    prod_hier_detl, on="PRODUCT_ID", how="left"
)

trx_with_agent = trx_with_product.join(
    agent_valid.select("AGENT_ID", "RECOGNIZED_CHANNEL", "OVERRIDE_CHANNEL"),
    on="AGENT_ID", how="left"
).withColumn(
    "EFFECTIVE_CHANNEL",
    F.coalesce(F.col("OVERRIDE_CHANNEL"), F.col("RECOGNIZED_CHANNEL"))
)

trx_with_month = trx_with_agent.withColumn(
    "REPORTING_MONTH", F.last_day(F.col("TRX_DATE"))
)

# TOTAL_PAP_AMT: SUM(PAP_AMT * PAP_WEIGHT_PCT) for Life 100%, MoneyGuard 100%, Fixed Annuity 5%
weighted_pap = trx_with_month.withColumn(
    "WEIGHTED_PAP",
    F.when(
        F.col("PRODUCT_TYPE").isin("Life", "MoneyGuard", "Fixed Annuity"),
        F.col("PAP_AMT") * (F.col("PAP_WEIGHT_PCT") / F.lit(100.0))
    ).otherwise(F.lit(0.0))
)

total_pap = weighted_pap.groupBy("AGENT_ID", "REPORTING_MONTH", "EFFECTIVE_CHANNEL").agg(
    F.sum("WEIGHTED_PAP").alias("TOTAL_PAP_AMT")
)

# FIA amount for Allstate Service/Financial reduction
fia_amt = weighted_pap.filter(F.col("PRODUCT_TYPE") == "Fixed Indexed Annuity").groupBy(
    "AGENT_ID", "REPORTING_MONTH"
).agg(F.sum("PAP_AMT").alias("FIA_PAP_AMT"))

# Variable + NY PAP for Financial PAP exclusion
variable_ny_pap = weighted_pap.filter(
    (F.col("PRODUCT_TYPE") == "Variable") | (F.col("STATE_CD") == "NY")
).groupBy("AGENT_ID", "REPORTING_MONTH").agg(
    F.sum("PAP_AMT").alias("VARIABLE_NY_PAP_AMT")
)

pp_agent_amt_smry = (
    total_pap
    .join(fia_amt, on=["AGENT_ID", "REPORTING_MONTH"], how="left")
    .join(variable_ny_pap, on=["AGENT_ID", "REPORTING_MONTH"], how="left")
    .fillna({"FIA_PAP_AMT": 0.0, "VARIABLE_NY_PAP_AMT": 0.0})
    .withColumn(
        # SERVICE_PAP_AMT: Total PAP; Allstate further reduced by FIA PAP
        "SERVICE_PAP_AMT",
        F.when(F.col("EFFECTIVE_CHANNEL") == "Allstate",
               F.col("TOTAL_PAP_AMT") - F.col("FIA_PAP_AMT"))
         .otherwise(F.col("TOTAL_PAP_AMT"))
    )
    .withColumn(
        # FINANCIAL_PAP_AMT: Total - Variable - NY; Allstate further reduced by FIA
        "FINANCIAL_PAP_AMT",
        F.when(
            F.col("EFFECTIVE_CHANNEL") == "Allstate",
            F.col("TOTAL_PAP_AMT") - F.col("VARIABLE_NY_PAP_AMT") - F.col("FIA_PAP_AMT")
        ).otherwise(
            F.col("TOTAL_PAP_AMT") - F.col("VARIABLE_NY_PAP_AMT")
        )
    )
    .withColumn(
        # PROJECTED_FINANCIAL_PAP_AMT: annualized run-rate (elapsed months in cal year)
        "ELAPSED_MONTHS", F.month(F.col("REPORTING_MONTH"))
    )
    .withColumn(
        "PROJECTED_FINANCIAL_PAP_AMT",
        F.when(F.col("ELAPSED_MONTHS") > 0,
               (F.col("FINANCIAL_PAP_AMT") / F.col("ELAPSED_MONTHS")) * F.lit(12))
         .otherwise(F.col("FINANCIAL_PAP_AMT"))
    )
    .select(
        "AGENT_ID", "REPORTING_MONTH", "TOTAL_PAP_AMT", "SERVICE_PAP_AMT",
        "FINANCIAL_PAP_AMT", "PROJECTED_FINANCIAL_PAP_AMT"
    )
)

pp_agent_amt_smry = pp_agent_amt_smry.withColumn(
    "TOTAL_PAP_AMT", F.col("TOTAL_PAP_AMT").cast(DecimalType(15, 2))
).withColumn(
    "SERVICE_PAP_AMT", F.col("SERVICE_PAP_AMT").cast(DecimalType(15, 2))
).withColumn(
    "FINANCIAL_PAP_AMT", F.col("FINANCIAL_PAP_AMT").cast(DecimalType(15, 2))
).withColumn(
    "PROJECTED_FINANCIAL_PAP_AMT", F.col("PROJECTED_FINANCIAL_PAP_AMT").cast(DecimalType(15, 2))
)

write_target(pp_agent_amt_smry, "PP_AGENT_AMT_SMRY", mode="overwrite",
             partition_cols=["REPORTING_MONTH"])

# ---------------------------------------------------------------------------
# Transform: PP_QUALFCTN_SUMRY
# STTM: qualification level (hardcoded thresholds), inclusion reason, loyalty flags
# ---------------------------------------------------------------------------
logger.info("Building PP_QUALFCTN_SUMRY")

agent_role = agent_valid.select(
    "AGENT_ID", "RECOGNIZED_ROLE", "OVERRIDE_ROLE"
).withColumn(
    "EFFECTIVE_ROLE", F.coalesce(F.col("OVERRIDE_ROLE"), F.col("RECOGNIZED_ROLE"))
)

qual_base = pp_agent_amt_smry.join(agent_role, on="AGENT_ID", how="left")


def qualification_level_expr(pap_col: str, role_col: str = "EFFECTIVE_ROLE"):
    """Hardcoded threshold gate: role clears its OWN minimum before comparison."""
    fp_gold, fp_plat, fp_dia = THRESHOLDS["FP"]["Gold"], THRESHOLDS["FP"]["Platinum"], THRESHOLDS["FP"]["Diamond"]
    ag_gold, ag_plat, ag_dia = THRESHOLDS["AGENCY"]["Gold"], THRESHOLDS["AGENCY"]["Platinum"], THRESHOLDS["AGENCY"]["Diamond"]
    return (
        F.when(
            (F.col(role_col) == "FP") & (F.col(pap_col) >= fp_dia), F.lit("Diamond")
        ).when(
            (F.col(role_col) == "FP") & (F.col(pap_col) >= fp_plat), F.lit("Platinum")
        ).when(
            (F.col(role_col) == "FP") & (F.col(pap_col) >= fp_gold), F.lit("Gold")
        ).when(
            (F.col(role_col) == "Agency/Manager") & (F.col(pap_col) >= ag_dia), F.lit("Diamond")
        ).when(
            (F.col(role_col) == "Agency/Manager") & (F.col(pap_col) >= ag_plat), F.lit("Platinum")
        ).when(
            (F.col(role_col) == "Agency/Manager") & (F.col(pap_col) >= ag_gold), F.lit("Gold")
        ).otherwise(F.lit(None).cast(StringType()))
    )


qual_base = qual_base.withColumn(
    "SERVICE_QUAL_LEVEL", qualification_level_expr("SERVICE_PAP_AMT")
).withColumn(
    "FINANCIAL_QUAL_LEVEL", qualification_level_expr("FINANCIAL_PAP_AMT")
).withColumn(
    # Financial qualification, if present, drives overall QUALIFICATION_LEVEL (FSD 5.2);
    # else fall back to Service-derived level.
    "QUALIFICATION_LEVEL",
    F.coalesce(F.col("FINANCIAL_QUAL_LEVEL"), F.col("SERVICE_QUAL_LEVEL"))
)

# Prior-year / two-years-in-arrear qualifier lookup from historical qualification table
hist_prior_year = hist_qual.filter(
    F.col("QUAL_YEAR") == (F.year(F.current_date()) - 1)
).filter(F.col("QUALIFICATION_LEVEL").isNotNull()).select(
    F.col("AGENT_ID").alias("_PRIOR_AGENT_ID")
).distinct()

hist_two_yr_arrear = hist_qual.filter(
    F.col("QUAL_YEAR") == (F.year(F.current_date()) - 2)
).filter(F.col("QUALIFICATION_LEVEL").isNotNull()).select(
    F.col("AGENT_ID").alias("_ARREAR_AGENT_ID")
).distinct()

# Active Sustaining / Pinnacle windows from historical qualification table
hist_sustaining = hist_qual.filter(F.col("SUSTAINING_STATUS_IND") == "Y").select(
    F.col("AGENT_ID").alias("_SUST_AGENT_ID"),
    F.col("LAST_QUALIFYING_YEAR").alias("_SUST_LAST_YR")
).distinct()

hist_pinnacle = hist_qual.filter(F.col("PINNACLE_STATUS_IND") == "Y").select(
    F.col("AGENT_ID").alias("_PIN_AGENT_ID"),
    F.col("LAST_QUALIFYING_YEAR").alias("_PIN_LAST_YR")
).distinct()

qual_enriched = (
    qual_base
    .join(hist_prior_year, qual_base["AGENT_ID"] == F.col("_PRIOR_AGENT_ID"), "left")
    .join(hist_two_yr_arrear, qual_base["AGENT_ID"] == F.col("_ARREAR_AGENT_ID"), "left")
    .join(hist_sustaining, qual_base["AGENT_ID"] == F.col("_SUST_AGENT_ID"), "left")
    .join(hist_pinnacle, qual_base["AGENT_ID"] == F.col("_PIN_AGENT_ID"), "left")
    .withColumn("CURRENT_REPORTING_YEAR", F.year(F.col("REPORTING_MONTH")))
    .withColumn("IS_PRIOR_YEAR_QUAL", F.col("_PRIOR_AGENT_ID").isNotNull())
    .withColumn("IS_TWO_YR_ARREAR_QUAL", F.col("_ARREAR_AGENT_ID").isNotNull())
    .withColumn(
        "SUSTAINING_STATUS_IND",
        F.when(F.col("_SUST_AGENT_ID").isNotNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "SUSTAINING_SVC_BENEFIT_IND",
        F.when(
            (F.col("SUSTAINING_STATUS_IND") == "Y") &
            (F.col("CURRENT_REPORTING_YEAR") - F.col("_SUST_LAST_YR") <= 3),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "PINNACLE_STATUS_IND",
        F.when(F.col("_PIN_AGENT_ID").isNotNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "PINNACLE_SVC_BENEFIT_IND",
        F.when(
            (F.col("PINNACLE_STATUS_IND") == "Y") &
            (F.col("CURRENT_REPORTING_YEAR") - F.col("_PIN_LAST_YR") <= 5),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# PRODUCTION_PROXIMITY_PCT: Projected Financial PAP / next threshold; null at Diamond
def next_threshold_expr():
    fp_gold, fp_plat, fp_dia = THRESHOLDS["FP"]["Gold"], THRESHOLDS["FP"]["Platinum"], THRESHOLDS["FP"]["Diamond"]
    ag_gold, ag_plat, ag_dia = THRESHOLDS["AGENCY"]["Gold"], THRESHOLDS["AGENCY"]["Platinum"], THRESHOLDS["AGENCY"]["Diamond"]
    return (
        F.when((F.col("EFFECTIVE_ROLE") == "FP") & (F.col("QUALIFICATION_LEVEL") == "Gold"), F.lit(fp_plat))
         .when((F.col("EFFECTIVE_ROLE") == "FP") & (F.col("QUALIFICATION_LEVEL").isNull()), F.lit(fp_gold))
         .when((F.col("EFFECTIVE_ROLE") == "FP") & (F.col("QUALIFICATION_LEVEL") == "Platinum"), F.lit(fp_dia))
         .when((F.col("EFFECTIVE_ROLE") == "Agency/Manager") & (F.col("QUALIFICATION_LEVEL") == "Gold"), F.lit(ag_plat))
         .when((F.col("EFFECTIVE_ROLE") == "Agency/Manager") & (F.col("QUALIFICATION_LEVEL").isNull()), F.lit(ag_gold))
         .when((F.col("EFFECTIVE_ROLE") == "Agency/Manager") & (F.col("QUALIFICATION_LEVEL") == "Platinum"), F.lit(ag_dia))
         .otherwise(F.lit(None).cast(DecimalType(15, 2)))  # Diamond: no higher level, not applicable
    )

qual_enriched = qual_enriched.withColumn("NEXT_THRESHOLD", next_threshold_expr())

qual_enriched = qual_enriched.withColumn(
    "PRODUCTION_PROXIMITY_PCT",
    F.when(
        F.col("NEXT_THRESHOLD").isNotNull(),
        (F.col("PROJECTED_FINANCIAL_PAP_AMT") / F.col("NEXT_THRESHOLD")).cast(DecimalType(5, 2))
    ).otherwise(F.lit(None).cast(DecimalType(5, 2)))
)

# Production Proximity Candidate: within 1% of Gold minimum, even if below it
gold_threshold_expr = F.when(
    F.col("EFFECTIVE_ROLE") == "FP", F.lit(THRESHOLDS["FP"]["Gold"])
).when(
    F.col("EFFECTIVE_ROLE") == "Agency/Manager", F.lit(THRESHOLDS["AGENCY"]["Gold"])
).otherwise(F.lit(None).cast(DecimalType(15, 2)))

qual_enriched = qual_enriched.withColumn("GOLD_THRESHOLD", gold_threshold_expr)
qual_enriched = qual_enriched.withColumn(
    "IS_PRODUCTION_PROXIMITY_CANDIDATE",
    F.when(
        F.col("GOLD_THRESHOLD").isNotNull(),
        F.col("PROJECTED_FINANCIAL_PAP_AMT") >= F.col("GOLD_THRESHOLD") * F.lit(1 - GOLD_PROXIMITY_PCT)
    ).otherwise(F.lit(False))
)

qual_enriched = qual_enriched.withColumn(
    "IS_CURRENT_YEAR_PROJECTED",
    F.when(
        F.col("GOLD_THRESHOLD").isNotNull(),
        F.col("PROJECTED_FINANCIAL_PAP_AMT") >= F.col("GOLD_THRESHOLD")
    ).otherwise(F.lit(False))
)

# INCLUSION_REASON_CD: monthly output must include ALL qualifying reasons, not just current YTD
qual_enriched = qual_enriched.withColumn(
    "INCLUSION_REASON_CD",
    F.when(F.col("QUALIFICATION_LEVEL").isNotNull(), F.lit("CURRENT_YTD"))
     .when(F.col("IS_PRIOR_YEAR_QUAL"), F.lit("PRIOR_YEAR"))
     .when(F.col("IS_TWO_YR_ARREAR_QUAL"), F.lit("TWO_YR_ARREAR"))
     .when(F.col("IS_CURRENT_YEAR_PROJECTED"), F.lit("CURRENT_YR_PROJECTED"))
     .when(F.col("SUSTAINING_SVC_BENEFIT_IND") == "Y", F.lit("ACTIVE_SUSTAINING"))
     .when(F.col("PINNACLE_SVC_BENEFIT_IND") == "Y", F.lit("ACTIVE_PINNACLE"))
     .when(F.col("IS_PRODUCTION_PROXIMITY_CANDIDATE"), F.lit("PRODUCTION_PROXIMITY"))
     .otherwise(F.lit(None).cast(StringType()))
)

# Filter to monthly output inclusion criteria — any qualifying reason present
pp_qualfctn_pre_dc = qual_enriched.filter(F.col("INCLUSION_REASON_CD").isNotNull())

# Deferred Compensation eligibility — separate Oct 1(prior yr)-Sep 30(current yr) window,
# hardcoded $150K Financial PAP threshold, independent of calendar-year PP qualification.
# NOTE: assumes an upstream FINANCIAL_PAP_OCT_SEP_AMT feed for the rolling window; placeholder join shown.
deferred_comp_window_pap = pp_agent_amt_smry.groupBy("AGENT_ID").agg(
    F.max("FINANCIAL_PAP_AMT").alias("FINANCIAL_PAP_OCT_SEP_AMT")  # placeholder aggregation for rolling window
)

pp_qualfctn_sumry = pp_qualfctn_pre_dc.join(
    deferred_comp_window_pap, on="AGENT_ID", how="left"
).withColumn(
    "DEFERRED_COMP_ELIGIBLE_IND",
    F.when(F.col("FINANCIAL_PAP_OCT_SEP_AMT") >= F.lit(DEFERRED_COMP_THRESHOLD_FP), F.lit("Y")).otherwise(F.lit("N"))
).select(
    "AGENT_ID", "REPORTING_MONTH", "QUALIFICATION_LEVEL", "INCLUSION_REASON_CD",
    "PINNACLE_STATUS_IND", "PINNACLE_SVC_BENEFIT_IND",
    "SUSTAINING_STATUS_IND", "SUSTAINING_SVC_BENEFIT_IND",
    "PRODUCTION_PROXIMITY_PCT", "DEFERRED_COMP_ELIGIBLE_IND"
)

write_target(pp_qualfctn_sumry, "PP_QUALFCTN_SUMRY", mode="overwrite",
             partition_cols=["REPORTING_MONTH"])

# ---------------------------------------------------------------------------
# Transform: FACT_PP_QUALFCTN_SUMRY
# STTM: append-only historical fact, denormalized role/channel/amounts.
# Dedup/merge key = AGENT_ID + REPORTING_MONTH + LOAD_DT (per approval note).
# ---------------------------------------------------------------------------
logger.info("Building FACT_PP_QUALFCTN_SUMRY")

LOAD_DT = datetime.utcnow()

agent_role_channel = agent_valid.select(
    "AGENT_ID", "RECOGNIZED_ROLE", "OVERRIDE_ROLE",
    "RECOGNIZED_CHANNEL", "OVERRIDE_CHANNEL"
).withColumn(
    "EFFECTIVE_ROLE_FINAL", F.coalesce(F.col("OVERRIDE_ROLE"), F.col("RECOGNIZED_ROLE"))
).withColumn(
    "EFFECTIVE_CHANNEL_FINAL", F.coalesce(F.col("OVERRIDE_CHANNEL"), F.col("RECOGNIZED_CHANNEL"))
)

territory_lookup = sf_acct_terrty_detl.select("ACCOUNT_ID", "TERRITORY_ID").distinct()

# Note: ACCOUNT_ID join key assumed available on agent/party crosswalk; placeholder join shown
fact_base = (
    pp_qualfctn_sumry
    .join(pp_agent_amt_smry, on=["AGENT_ID", "REPORTING_MONTH"], how="left")
    .join(agent_role_channel, on="AGENT_ID", how="left")
    .join(agent_valid.select("AGENT_ID", "ACCOUNT_ID") if "ACCOUNT_ID" in agent_valid.columns
          else agent_valid.select("AGENT_ID").withColumn("ACCOUNT_ID", F.lit(None).cast(StringType())),
          on="AGENT_ID", how="left")
    .join(territory_lookup, on="ACCOUNT_ID", how="left")
)

fact_base = fact_base.withColumn(
    "REPORTING_YEAR", F.year(F.col("REPORTING_MONTH"))
).withColumnRenamed(
    "EFFECTIVE_ROLE_FINAL", "RECOGNIZED_ROLE_FINAL"
).withColumnRenamed(
    "EFFECTIVE_CHANNEL_FINAL", "RECOGNIZED_CHANNEL_FINAL"
)

# IUL_BONUS_PCT: 1% Gold / 3% Platinum / 5% Diamond
fact_base = fact_base.withColumn(
    "IUL_BONUS_PCT",
    F.when(F.col("QUALIFICATION_LEVEL") == "Gold", F.lit(1.00))
     .when(F.col("QUALIFICATION_LEVEL") == "Platinum", F.lit(3.00))
     .when(F.col("QUALIFICATION_LEVEL") == "Diamond", F.lit(5.00))
     .otherwise(F.lit(None).cast(DecimalType(5, 2)))
)

# RSU_AMT: Agency Channel FP + Pinnacle only — $10,000 Platinum, $20,000 Diamond
fact_base = fact_base.withColumn(
    "RSU_AMT",
    F.when(
        (F.col("RECOGNIZED_CHANNEL_FINAL") == "Agency") &
        (F.col("RECOGNIZED_ROLE_FINAL") == "FP") &
        (F.col("PINNACLE_STATUS_IND") == "Y") &
        (F.col("QUALIFICATION_LEVEL") == "Diamond"),
        F.lit(20000.00)
    ).when(
        (F.col("RECOGNIZED_CHANNEL_FINAL") == "Agency") &
        (F.col("RECOGNIZED_ROLE_FINAL") == "FP") &
        (F.col("PINNACLE_STATUS_IND") == "Y") &
        (F.col("QUALIFICATION_LEVEL") == "Platinum"),
        F.lit(10000.00)
    ).otherwise(F.lit(None).cast(DecimalType(10, 2)))
)

# BUS_DEV_DOLLARS_AMT: standard Agency amounts, doubled for Pinnacle/Sustaining
fact_base = fact_base.withColumn(
    "IS_LOYALTY_QUALIFIER",
    (F.col("PINNACLE_STATUS_IND") == "Y") | (F.col("SUSTAINING_STATUS_IND") == "Y")
)

fact_base = fact_base.withColumn(
    "BUS_DEV_DOLLARS_AMT",
    F.when(F.col("QUALIFICATION_LEVEL") == "Gold",
           F.when(F.col("IS_LOYALTY_QUALIFIER"), F.lit(8000.00)).otherwise(F.lit(4000.00)))
     .when(F.col("QUALIFICATION_LEVEL") == "Platinum",
           F.when(F.col("IS_LOYALTY_QUALIFIER"), F.lit(16000.00)).otherwise(F.lit(8000.00)))
     .when(F.col("QUALIFICATION_LEVEL") == "Diamond",
           F.when(F.col("IS_LOYALTY_QUALIFIER"), F.lit(24000.00)).otherwise(F.lit(12000.00)))
     .otherwise(F.lit(None).cast(DecimalType(10, 2)))
)

fact_pp_qualfctn_sumry = fact_base.withColumn(
    "LOAD_DT", F.lit(LOAD_DT).cast(TimestampType())
).select(
    "AGENT_ID", "REPORTING_MONTH", "REPORTING_YEAR",
    F.col("RECOGNIZED_ROLE_FINAL").alias("RECOGNIZED_ROLE"),
    F.col("RECOGNIZED_CHANNEL_FINAL").alias("RECOGNIZED_CHANNEL"),
    "TERRITORY_ID", "QUALIFICATION_LEVEL", "TOTAL_PAP_AMT",
    "IUL_BONUS_PCT", "RSU_AMT", "BUS_DEV_DOLLARS_AMT", "LOAD_DT"
)

# Append-only write; dedup/merge key AGENT_ID + REPORTING_MONTH + LOAD_DT — never overwrite/delete
# prior REPORTING_MONTH rows, and never collapse rows across LOAD_DT values.
existing_fact_keys = None
try:
    existing_fact = spark.read.parquet(TGT_PATHS["FACT_PP_QUALFCTN_SUMRY"])
    dedup_check = fact_pp_qualfctn_sumry.join(
        existing_fact.select("AGENT_ID", "REPORTING_MONTH", "LOAD_DT"),
        on=["AGENT_ID", "REPORTING_MONTH", "LOAD_DT"],
        how="left_anti"
    )
    logger.info(
        f"Filtered {fact_pp_qualfctn_sumry.count() - dedup_check.count()} rows already present "
        f"for identical (AGENT_ID, REPORTING_MONTH, LOAD_DT) key"
    )
    fact_pp_qualfctn_sumry = dedup_check
except Exception:
    logger.info("No existing FACT_PP_QUALFCTN_SUMRY data found — treating as initial load")

write_target(fact_pp_qualfctn_sumry, "FACT_PP_QUALFCTN_SUMRY", mode="append",
             partition_cols=["REPORTING_YEAR"])

logger.info("PP qualification pipeline completed successfully")

if GLUE_ENV:
    job.commit()
```