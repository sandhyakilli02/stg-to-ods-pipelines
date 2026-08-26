```python
"""
Greenfield ODS Load: Premier Partner Agent, Certification & Tier Data
Source Systems: SalesIQ CRM, Premier Partner Portal
Targets: DIM_PP_AGENT (SCD2), DIM_PP_CERTIFICATION (SCD1), REF_PP_TIER (SCD0)
Jira: KAN-33
"""

import sys
import logging
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, TimestampType,
    BooleanType, IntegerType, LongType, DecimalType
)

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------------------------
# Logging setup (Glue-compatible logger)
# ---------------------------------------------------------------------------
logger = logging.getLogger("KAN33_PP_ODS_LOAD")
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Spark / Glue context bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ETL_BATCH_ID"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

ETL_BATCH_ID = args["ETL_BATCH_ID"]
LOAD_TS = datetime.utcnow()
LOAD_DATE = LOAD_TS.date()

# ---------------------------------------------------------------------------
# S3 source / target placeholders
# ---------------------------------------------------------------------------
SRC_AGENT_PATH = "s3://raw-zone/premier-partner-portal/pp_agent_profile/"
SRC_CERT_PATH = "s3://raw-zone/premier-partner-portal/pp_certification/"
SRC_TIER_PATH = "s3://raw-zone/premier-partner-portal/ref_tier/"

TGT_DIM_AGENT_PATH = "s3://ods-zone/dim_pp_agent/"
TGT_DIM_CERT_PATH = "s3://ods-zone/dim_pp_certification/"
TGT_REF_TIER_PATH = "s3://ods-zone/ref_pp_tier/"
REJECT_CERT_PATH = "s3://ods-zone/rejects/dim_pp_certification/"
DQ_WARNING_PATH = "s3://ods-zone/dq_warnings/"

STATUS_MAP = {"A": "ACTIVE", "I": "INACTIVE", "P": "PENDING"}


def map_status_cd(df: DataFrame, src_col: str = "STATUS_CD") -> DataFrame:
    """STTM rule: map STATUS_CD 'A'->ACTIVE, 'I'->INACTIVE, 'P'->PENDING.
    Unmapped codes are flagged for the DQ reject queue rather than defaulted."""
    mapping_expr = F.create_map([F.lit(x) for pair in STATUS_MAP.items() for x in pair])
    return df.withColumn(
        "STATUS_CD_MAPPED", mapping_expr.getItem(F.col(src_col))
    ).withColumn(
        "STATUS_CD_VALID", F.col("STATUS_CD_MAPPED").isNotNull()
    )


def read_source(path: str, schema: StructType, label: str) -> DataFrame:
    """Read a source dataset with basic error handling/logging."""
    try:
        df = spark.read.schema(schema).parquet(path)
        logger.info(f"Loaded {label} from {path} - {df.count()} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to read source {label} at {path}: {e}")
        raise


# ---------------------------------------------------------------------------
# Source schemas
# ---------------------------------------------------------------------------
agent_schema = StructType([
    StructField("AGENT_ID", StringType(), False),
    StructField("AGENT_NM", StringType(), True),
    StructField("AGENT_EMAIL", StringType(), True),
    StructField("TIER_CD", StringType(), True),
    StructField("ONBOARD_DT", DateType(), True),
    StructField("STATUS_CD", StringType(), True),
    StructField("REGION_CD", StringType(), True),
    StructField("MGR_AGENT_ID", StringType(), True),
])

cert_schema = StructType([
    StructField("CERT_ID", StringType(), False),
    StructField("AGENT_ID", StringType(), True),
    StructField("CERT_TYPE_CD", StringType(), True),
    StructField("CERT_DT", DateType(), True),
    StructField("EXPIRY_DT", DateType(), True),
    StructField("STATUS_CD", StringType(), True),
])

tier_schema = StructType([
    StructField("TIER_CD", StringType(), False),
    StructField("TIER_NM", StringType(), True),
    StructField("TIER_DESC", StringType(), True),
    StructField("MIN_PREMIUM_AMT", DecimalType(18, 2), True),
    StructField("MAX_PREMIUM_AMT", DecimalType(18, 2), True),
])

# ---------------------------------------------------------------------------
# DIM_PP_AGENT :: SCD Type 2 load
# ---------------------------------------------------------------------------
def build_dim_pp_agent(src_agent: DataFrame, existing_dim: DataFrame = None) -> DataFrame:
    """
    STTM: DIM_PP_AGENT - SCD Type 2.
    - REGION_CD defaults to 'UNKNOWN' if NULL.
    - STATUS_CD mapped A/I/P -> ACTIVE/INACTIVE/PENDING.
    - AGENT_SK = hash(AGENT_ID + EFFECTIVE_DT).
    - New version row inserted only when TIER_CD changes vs current row.
    """
    df = src_agent.dropDuplicates(["AGENT_ID"])  # DQ: AGENT_ID must be unique per extract

    df = df.withColumn(
        "REGION_CD", F.when(F.col("REGION_CD").isNull() | (F.trim(F.col("REGION_CD")) == ""),
                             F.lit("UNKNOWN")).otherwise(F.col("REGION_CD"))
    )

    df = map_status_cd(df, "STATUS_CD")
    rejected_status = df.filter(~F.col("STATUS_CD_VALID"))
    if rejected_status.count() > 0:
        logger.warning(f"{rejected_status.count()} PP_AGENT_PROFILE rows have unmapped STATUS_CD")
        rejected_status.write.mode("append").parquet(DQ_WARNING_PATH + "agent_status_cd/")
    df = df.filter(F.col("STATUS_CD_VALID")).drop("STATUS_CD_VALID")
    df = df.withColumn("STATUS_CD", F.col("STATUS_CD_MAPPED")).drop("STATUS_CD_MAPPED")

    df = df.withColumn("EFFECTIVE_DT", F.lit(LOAD_DATE)) \
           .withColumn("EXPIRY_DT", F.lit("9999-12-31").cast(DateType())) \
           .withColumn("IS_CURRENT_FL", F.lit(True))

    if existing_dim is None:
        # Greenfield: first load, all agents get VERSION_NO = 1
        df = df.withColumn("VERSION_NO", F.lit(1))
    else:
        current = existing_dim.filter(F.col("IS_CURRENT_FL") == True)  # noqa: E712
        joined = df.alias("src").join(
            current.select("AGENT_ID", "TIER_CD", "VERSION_NO").alias("cur"),
            on="AGENT_ID", how="left"
        )
        changed = joined.filter(
            F.col("cur.TIER_CD").isNull() | (F.col("src.TIER_CD") != F.col("cur.TIER_CD"))
        )
        # DQ: expire prior current row for changed agents (handled in merge step downstream)
        df = changed.withColumn(
            "VERSION_NO", F.coalesce(F.col("cur.VERSION_NO"), F.lit(0)) + 1
        ).select("src.*", "VERSION_NO")

    df = df.withColumn(
        "AGENT_SK", F.sha2(F.concat_ws("||", F.col("AGENT_ID"), F.col("EFFECTIVE_DT").cast(StringType())), 256)
    )

    return df.select(
        "AGENT_SK", "AGENT_ID", "AGENT_NM", "AGENT_EMAIL", "TIER_CD", "REGION_CD",
        "MGR_AGENT_ID", "ONBOARD_DT", "STATUS_CD", "EFFECTIVE_DT", "EXPIRY_DT",
        "IS_CURRENT_FL", "VERSION_NO"
    )


def soft_validate_agent_references(dim_agent: DataFrame, ref_tier: DataFrame) -> None:
    """DQ warning (non-blocking): MGR_AGENT_ID and TIER_CD referential checks."""
    orphan_mgrs = dim_agent.join(
        dim_agent.select(F.col("AGENT_ID").alias("MGR_LOOKUP")),
        dim_agent["MGR_AGENT_ID"] == F.col("MGR_LOOKUP"), "left_anti"
    ).filter(F.col("MGR_AGENT_ID").isNotNull())
    if orphan_mgrs.count() > 0:
        logger.warning(f"{orphan_mgrs.count()} agents reference an unknown MGR_AGENT_ID")
        orphan_mgrs.write.mode("append").parquet(DQ_WARNING_PATH + "orphan_manager_refs/")

    unmatched_tiers = dim_agent.join(ref_tier, "TIER_CD", "left_anti")
    if unmatched_tiers.count() > 0:
        logger.warning(f"{unmatched_tiers.count()} agents reference an unknown TIER_CD")
        unmatched_tiers.write.mode("append").parquet(DQ_WARNING_PATH + "unmatched_tier_refs/")


# ---------------------------------------------------------------------------
# DIM_PP_CERTIFICATION :: SCD Type 1 load
# ---------------------------------------------------------------------------
def build_dim_pp_certification(src_cert: DataFrame, existing_dim: DataFrame = None) -> DataFrame:
    """
    STTM: DIM_PP_CERTIFICATION - SCD Type 1.
    - Reject rows where EXPIRY_DT is null or EXPIRY_DT <= CERT_DT.
    - Overwrite in place on renewal; UPDATE_DT refreshed, INSERT_DT preserved.
    """
    df = src_cert.dropDuplicates(["CERT_ID"])  # DQ: CERT_ID must be unique per extract

    df = df.withColumn(
        "IS_VALID_DATE_RANGE",
        F.col("EXPIRY_DT").isNotNull() & (F.col("EXPIRY_DT") > F.col("CERT_DT"))
    )
    rejected = df.filter(~F.col("IS_VALID_DATE_RANGE")) \
                 .withColumn("REJECT_REASON", F.lit("EXPIRY_DT <= CERT_DT or NULL")) \
                 .withColumn("REJECT_LOAD_TS", F.lit(LOAD_TS))
    reject_count = rejected.count()
    if reject_count > 0:
        logger.warning(f"{reject_count} PP_CERTIFICATION rows rejected on date validation")
        rejected.select("CERT_ID", "AGENT_ID", "REJECT_REASON", "REJECT_LOAD_TS") \
                .write.mode("append").parquet(REJECT_CERT_PATH)

    df = df.filter(F.col("IS_VALID_DATE_RANGE")).drop("IS_VALID_DATE_RANGE")

    df = map_status_cd(df, "STATUS_CD")
    invalid_status = df.filter(~F.col("STATUS_CD_VALID"))
    if invalid_status.count() > 0:
        logger.warning(f"{invalid_status.count()} PP_CERTIFICATION rows have unmapped STATUS_CD")
        invalid_status.write.mode("append").parquet(DQ_WARNING_PATH + "cert_status_cd/")
    df = df.filter(F.col("STATUS_CD_VALID")).drop("STATUS_CD_VALID")
    df = df.withColumn("STATUS_CD", F.col("STATUS_CD_MAPPED")).drop("STATUS_CD_MAPPED")

    df = df.withColumn("CERT_SK", F.sha2(F.col("CERT_ID"), 256)) \
           .withColumn("UPDATE_DT", F.lit(LOAD_TS)) \
           .withColumn("LOAD_DT", F.lit(LOAD_TS)) \
           .withColumn("ETL_BATCH_ID", F.lit(ETL_BATCH_ID))

    if existing_dim is None:
        df = df.withColumn("INSERT_DT", F.lit(LOAD_TS))
    else:
        prior_insert = existing_dim.select("CERT_ID", F.col("INSERT_DT").alias("PRIOR_INSERT_DT"))
        df = df.join(prior_insert, "CERT_ID", "left").withColumn(
            "INSERT_DT", F.coalesce(F.col("PRIOR_INSERT_DT"), F.lit(LOAD_TS))
        ).drop("PRIOR_INSERT_DT")

    return df.select(
        "CERT_SK", "CERT_ID", "AGENT_ID", "CERT_TYPE_CD", "CERT_DT", "EXPIRY_DT",
        "STATUS_CD", "INSERT_DT", "UPDATE_DT", "LOAD_DT", "ETL_BATCH_ID"
    )


# ---------------------------------------------------------------------------
# REF_PP_TIER :: SCD Type 0 load (static reference, insert-only)
# ---------------------------------------------------------------------------
def build_ref_pp_tier(src_tier: DataFrame, existing_ref: DataFrame = None) -> DataFrame:
    """
    STTM: REF_PP_TIER - SCD Type 0.
    - Insert-only: existing rows are never updated.
    - DQ warning (non-blocking): MIN_PREMIUM_AMT should be < MAX_PREMIUM_AMT.
    """
    df = src_tier.dropDuplicates(["TIER_CD"])

    invalid_range = df.filter(F.col("MIN_PREMIUM_AMT") >= F.col("MAX_PREMIUM_AMT"))
    if invalid_range.count() > 0:
        logger.warning(f"{invalid_range.count()} REF_TIER rows have MIN_PREMIUM_AMT >= MAX_PREMIUM_AMT")
        invalid_range.write.mode("append").parquet(DQ_WARNING_PATH + "tier_premium_range/")

    df = df.withColumn("LOAD_DT", F.lit(LOAD_TS)) \
           .withColumn("SRC_SYS_NM", F.lit("Premier Partner Portal"))

    if existing_ref is not None:
        # SCD0: only insert TIER_CD values not already present
        df = df.join(existing_ref.select("TIER_CD"), "TIER_CD", "left_anti")

    return df.select(
        "TIER_CD", "TIER_NM", "TIER_DESC", "MIN_PREMIUM_AMT", "MAX_PREMIUM_AMT",
        "LOAD_DT", "SRC_SYS_NM"
    )


def write_target(df: DataFrame, path: str, mode: str, label: str) -> None:
    """Write to target S3/Glue Catalog location with error handling."""
    try:
        df.write.mode(mode).parquet(path)
        logger.info(f"Wrote {df.count()} rows to {label} at {path} (mode={mode})")
    except Exception as e:
        logger.error(f"Failed to write target {label} at {path}: {e}")
        raise


def main():
    logger.info(f"Starting KAN-33 greenfield ODS load, batch_id={ETL_BATCH_ID}")

    src_agent = read_source(SRC_AGENT_PATH, agent_schema, "PP_AGENT_PROFILE")
    src_cert = read_source(SRC_CERT_PATH, cert_schema, "PP_CERTIFICATION")
    src_tier = read_source(SRC_TIER_PATH, tier_schema, "REF_TIER")

    # Greenfield mode: no existing target data to reconcile against
    dim_agent = build_dim_pp_agent(src_agent, existing_dim=None)
    ref_tier = build_ref_pp_tier(src_tier, existing_ref=None)
    dim_cert = build_dim_pp_certification(src_cert, existing_dim=None)

    soft_validate_agent_references(dim_agent, ref_tier)

    # Idempotency note: re-running this job against the same source extract
    # in greenfield mode should be guarded by an external run-once check;
    # in subsequent incremental runs, existing_dim/existing_ref should be
    # loaded from the target path and passed into the build_* functions.
    write_target(dim_agent, TGT_DIM_AGENT_PATH, "overwrite", "DIM_PP_AGENT")
    write_target(dim_cert, TGT_DIM_CERT_PATH, "overwrite", "DIM_PP_CERTIFICATION")
    write_target(ref_tier, TGT_REF_TIER_PATH, "overwrite", "REF_PP_TIER")

    logger.info("KAN-33 greenfield ODS load completed successfully")
    job.commit()


if __name__ == "__main__":
    main()
```
