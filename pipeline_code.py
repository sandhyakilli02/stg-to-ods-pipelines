```python
"""
PP Agent Onboarding — Greenfield ODS Load
Targets: DIM_PP_AGENT (SCD2), DIM_PP_CERTIFICATION (SCD1), REF_PP_TIER (SCD0)
Runtime: AWS Glue / EMR (PySpark)
"""

import sys
import logging
from datetime import datetime, date

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    TimestampType, BooleanType, IntegerType, DecimalType, LongType
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
logger = logging.getLogger("pp_agent_onboarding")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

# ---------------------------------------------------------------------------
# Spark / Glue session setup
# ---------------------------------------------------------------------------
if GLUE_ENV:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ETL_BATCH_ID"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    ETL_BATCH_ID = args["ETL_BATCH_ID"]
else:
    spark = SparkSession.builder.appName("pp_agent_onboarding").getOrCreate()
    ETL_BATCH_ID = datetime.utcnow().strftime("BATCH_%Y%m%d%H%M%S")

BATCH_LOAD_DATE = date.today()
BATCH_LOAD_TS = datetime.utcnow()

# ---------------------------------------------------------------------------
# S3 source / target placeholders
# ---------------------------------------------------------------------------
SRC_PP_AGENT_PROFILE = "s3://raw-zone/premier_partner_portal/pp_agent_profile/"
SRC_PP_CERTIFICATION = "s3://raw-zone/salesiq_crm/pp_certification/"
SRC_REF_TIER = "s3://raw-zone/premier_partner_portal/ref_tier/"

TGT_DIM_PP_AGENT = "s3://ods-zone/dim_pp_agent/"
TGT_DIM_PP_CERTIFICATION = "s3://ods-zone/dim_pp_certification/"
TGT_REF_PP_TIER = "s3://ods-zone/ref_pp_tier/"
TGT_ERROR_QUEUE = "s3://ods-zone/error_reject/"

EXISTING_DIM_PP_AGENT = "s3://ods-zone/dim_pp_agent/"  # existing table for SCD2 compare


def read_source(path: str, fmt: str = "parquet") -> DataFrame:
    """Read a source dataset; wrapped for consistent error handling/logging."""
    try:
        df = spark.read.format(fmt).load(path)
        logger.info(f"Read {df.count()} rows from {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read source at {path}: {e}")
        raise


def map_status_cd(col):
    """STTM rule: STATUS_CD map 'A'->'ACTIVE', 'I'->'INACTIVE', 'P'->'PENDING'."""
    return (
        F.when(col == "A", F.lit("ACTIVE"))
         .when(col == "I", F.lit("INACTIVE"))
         .when(col == "P", F.lit("PENDING"))
         .otherwise(F.lit(None).cast(StringType()))
    )


def is_valid_status(col):
    return col.isin("A", "I", "P")


# ---------------------------------------------------------------------------
# REF_PP_TIER — SCD Type 0 (append-only, static)
# ---------------------------------------------------------------------------
def build_ref_pp_tier() -> DataFrame:
    logger.info("Building REF_PP_TIER (SCD0)")
    src = read_source(SRC_REF_TIER)

    ref_tier = (
        src.select(
            F.col("TIER_CD").cast(StringType()).alias("TIER_CD"),
            F.col("TIER_NM").cast(StringType()).alias("TIER_NM"),
            F.col("TIER_DESC").cast(StringType()).alias("TIER_DESC"),
            F.col("MIN_PREMIUM_AMT").cast(DecimalType(15, 2)).alias("MIN_PREMIUM_AMT"),
            F.col("MAX_PREMIUM_AMT").cast(DecimalType(15, 2)).alias("MAX_PREMIUM_AMT"),
            F.lit(BATCH_LOAD_TS).cast(TimestampType()).alias("LOAD_DT"),
            F.lit("Premier Partner Portal").alias("SRC_SYS_NM"),
        )
        .filter(F.col("TIER_CD").isNotNull())
    )

    # SCD0: never update existing tier codes. Only append TIER_CDs not already present.
    try:
        existing = spark.read.format("parquet").load(TGT_REF_PP_TIER)
        existing_codes = existing.select("TIER_CD").distinct()
        new_rows = ref_tier.join(existing_codes, on="TIER_CD", how="left_anti")

        # DQ exception: flag tier codes that exist but arrived with changed attributes
        changed = (
            ref_tier.alias("new")
            .join(existing.alias("old"), on="TIER_CD", how="inner")
            .filter(
                (F.col("new.TIER_NM") != F.col("old.TIER_NM"))
                | (F.col("new.TIER_DESC") != F.col("old.TIER_DESC"))
                | (F.col("new.MIN_PREMIUM_AMT") != F.col("old.MIN_PREMIUM_AMT"))
                | (F.col("new.MAX_PREMIUM_AMT") != F.col("old.MAX_PREMIUM_AMT"))
            )
        )
        if changed.count() > 0:
            logger.warning(f"{changed.count()} REF_PP_TIER rows arrived with changed attributes — DQ exception, not applied (SCD0).")
    except Exception:
        logger.info("No existing REF_PP_TIER target found — treating all rows as new (initial load).")
        new_rows = ref_tier

    return new_rows


# ---------------------------------------------------------------------------
# DIM_PP_AGENT — SCD Type 2 (versioned on TIER_CD change)
# ---------------------------------------------------------------------------
def build_dim_pp_agent(ref_tier_valid_codes: DataFrame) -> DataFrame:
    logger.info("Building DIM_PP_AGENT (SCD2)")
    src = read_source(SRC_PP_AGENT_PROFILE)

    # Business rule: AGENT_ID must be non-null
    valid_agent_id = src.filter(F.col("AGENT_ID").isNotNull())
    rejected_null_id = src.filter(F.col("AGENT_ID").isNull())
    if rejected_null_id.count() > 0:
        write_rejects(rejected_null_id, "NULL_AGENT_ID")

    # Business rule: STATUS_CD must be in {A, I, P}
    rejected_status = valid_agent_id.filter(~is_valid_status(F.col("STATUS_CD")))
    if rejected_status.count() > 0:
        write_rejects(rejected_status, "INVALID_STATUS_CD")
    valid_agent_id = valid_agent_id.filter(is_valid_status(F.col("STATUS_CD")))

    staged = (
        valid_agent_id
        .withColumn("REGION_CD", F.when(F.col("REGION_CD").isNull() | (F.trim(F.col("REGION_CD")) == ""),
                                         F.lit("UNKNOWN")).otherwise(F.col("REGION_CD")))
        .withColumn("STATUS_CD", map_status_cd(F.col("STATUS_CD")))
    )

    # Business rule: TIER_CD must exist in REF_PP_TIER
    valid_tier = staged.join(
        ref_tier_valid_codes.select("TIER_CD").distinct(), on="TIER_CD", how="inner"
    )
    rejected_tier = staged.join(
        ref_tier_valid_codes.select("TIER_CD").distinct(), on="TIER_CD", how="left_anti"
    )
    if rejected_tier.count() > 0:
        write_rejects(rejected_tier, "TIER_CD_NOT_IN_REF_PP_TIER")

    # SCD2 versioning: compare against existing current version by AGENT_ID
    try:
        existing = spark.read.format("parquet").load(EXISTING_DIM_PP_AGENT).filter(
            F.col("IS_CURRENT_FL") == True  # noqa: E712
        )
        has_existing = True
    except Exception:
        logger.info("No existing DIM_PP_AGENT found — treating as initial full load.")
        existing = None
        has_existing = False

    if has_existing:
        joined = valid_tier.alias("new").join(
            existing.alias("old"), on="AGENT_ID", how="left"
        )

        # Rows where TIER_CD changed (or brand new agent) get a new version
        changed_or_new = joined.filter(
            F.col("old.AGENT_ID").isNull() | (F.col("new.TIER_CD") != F.col("old.TIER_CD"))
        )

        new_versions = (
            changed_or_new
            .select("new.*", F.col("old.VERSION_NO").alias("PRIOR_VERSION_NO"))
            .withColumn("VERSION_NO", F.coalesce(F.col("PRIOR_VERSION_NO"), F.lit(0)) + 1)
            .withColumn("EFFECTIVE_DT", F.lit(BATCH_LOAD_DATE))
            .withColumn("EXPIRY_DT", F.lit(date(9999, 12, 31)))
            .withColumn("IS_CURRENT_FL", F.lit(True))
            .drop("PRIOR_VERSION_NO")
        )

        # Expire prior current rows for agents that changed
        agents_versioned = new_versions.select("AGENT_ID").distinct()
        expired_rows = (
            existing.join(agents_versioned, on="AGENT_ID", how="inner")
            .withColumn("EXPIRY_DT", F.date_sub(F.lit(BATCH_LOAD_DATE), 1))
            .withColumn("IS_CURRENT_FL", F.lit(False))
        )

        # Unchanged current rows carry forward untouched
        unchanged_rows = existing.join(agents_versioned, on="AGENT_ID", how="left_anti")

        dim_pp_agent_full = new_versions.unionByName(expired_rows).unionByName(unchanged_rows)
    else:
        dim_pp_agent_full = (
            valid_tier
            .withColumn("VERSION_NO", F.lit(1))
            .withColumn("EFFECTIVE_DT", F.lit(BATCH_LOAD_DATE))
            .withColumn("EXPIRY_DT", F.lit(date(9999, 12, 31)))
            .withColumn("IS_CURRENT_FL", F.lit(True))
        )

    # Generate surrogate key: hash(AGENT_ID + EFFECTIVE_DT)
    dim_pp_agent_full = dim_pp_agent_full.withColumn(
        "AGENT_SK", F.abs(F.xxhash64(F.concat_ws("||", F.col("AGENT_ID"), F.col("EFFECTIVE_DT").cast(StringType()))))
    )

    # DQ flag (non-blocking): MGR_AGENT_ID not resolving to an existing AGENT_ID
    known_agent_ids = dim_pp_agent_full.select(F.col("AGENT_ID").alias("KNOWN_ID")).distinct()
    dq_orphan_managers = (
        dim_pp_agent_full.filter(F.col("MGR_AGENT_ID").isNotNull())
        .join(known_agent_ids, dim_pp_agent_full.MGR_AGENT_ID == known_agent_ids.KNOWN_ID, "left_anti")
    )
    if dq_orphan_managers.count() > 0:
        logger.warning(f"{dq_orphan_managers.count()} rows have MGR_AGENT_ID not resolving to a known AGENT_ID — flagged in DQ report, not rejected.")

    final_cols = [
        "AGENT_SK", "AGENT_ID", "AGENT_NM", "AGENT_EMAIL", "TIER_CD", "REGION_CD",
        "MGR_AGENT_ID", "ONBOARD_DT", "STATUS_CD", "EFFECTIVE_DT", "EXPIRY_DT",
        "IS_CURRENT_FL", "VERSION_NO",
    ]
    return dim_pp_agent_full.select(*final_cols)


# ---------------------------------------------------------------------------
# DIM_PP_CERTIFICATION — SCD Type 1 (overwrite on renewal)
# ---------------------------------------------------------------------------
def build_dim_pp_certification(dim_pp_agent_current: DataFrame) -> DataFrame:
    logger.info("Building DIM_PP_CERTIFICATION (SCD1)")
    src = read_source(SRC_PP_CERTIFICATION)

    # De-duplicate CERT_ID within extract, keep most recent by source timestamp
    if "SRC_TIMESTAMP" in src.columns:
        w = Window.partitionBy("CERT_ID").orderBy(F.col("SRC_TIMESTAMP").desc())
        src = src.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # Business rule: STATUS_CD must be valid
    rejected_status = src.filter(~is_valid_status(F.col("STATUS_CD")))
    if rejected_status.count() > 0:
        write_rejects(rejected_status, "INVALID_STATUS_CD")
    src = src.filter(is_valid_status(F.col("STATUS_CD")))

    # Business rule: EXPIRY_DT > CERT_DT, else reject
    rejected_dates = src.filter(~(F.col("EXPIRY_DT") > F.col("CERT_DT")))
    if rejected_dates.count() > 0:
        write_rejects(rejected_dates, "EXPIRY_DT_NOT_AFTER_CERT_DT")
    valid_dates = src.filter(F.col("EXPIRY_DT") > F.col("CERT_DT"))

    staged = valid_dates.withColumn("STATUS_CD", map_status_cd(F.col("STATUS_CD")))

    # Lookup current AGENT_SK from DIM_PP_AGENT (IS_CURRENT_FL = TRUE)
    current_agents = dim_pp_agent_current.filter(F.col("IS_CURRENT_FL") == True).select(  # noqa: E712
        "AGENT_ID", "AGENT_SK"
    )

    joined = staged.join(current_agents, on="AGENT_ID", how="left")
    orphans = joined.filter(F.col("AGENT_SK").isNull())
    if orphans.count() > 0:
        write_rejects(orphans, "AGENT_SK_LOOKUP_NO_MATCH_PENDING_REPROCESS")
    matched = joined.filter(F.col("AGENT_SK").isNotNull())

    dim_pp_cert = (
        matched
        .withColumn("CERT_SK", F.abs(F.xxhash64(F.col("CERT_ID"))))
        .withColumn("INSERT_DT", F.lit(BATCH_LOAD_TS).cast(TimestampType()))
        .withColumn("UPDATE_DT", F.lit(BATCH_LOAD_TS).cast(TimestampType()))
        .withColumn("LOAD_DT", F.lit(BATCH_LOAD_TS).cast(TimestampType()))
        .withColumn("ETL_BATCH_ID", F.lit(ETL_BATCH_ID))
    )

    final_cols = [
        "CERT_SK", "CERT_ID", "AGENT_SK", "CERT_TYPE_CD", "CERT_DT", "EXPIRY_DT",
        "STATUS_CD", "INSERT_DT", "UPDATE_DT", "LOAD_DT", "ETL_BATCH_ID",
    ]
    return dim_pp_cert.select(*final_cols)


def write_rejects(df: DataFrame, reason_code: str) -> None:
    """Write rejected rows to the error/reject queue with reason code, for audit/reprocessing."""
    try:
        out = (
            df.withColumn("REJECTION_REASON_CD", F.lit(reason_code))
              .withColumn("REJECTED_AT", F.lit(BATCH_LOAD_TS).cast(TimestampType()))
              .withColumn("ETL_BATCH_ID", F.lit(ETL_BATCH_ID))
        )
        out.write.mode("append").format("parquet").save(TGT_ERROR_QUEUE)
        logger.warning(f"Wrote {out.count()} rows to error queue with reason '{reason_code}'.")
    except Exception as e:
        logger.error(f"Failed to write rejects for reason '{reason_code}': {e}")
        raise


def write_target(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    try:
        df.write.mode(mode).format("parquet").save(path)
        logger.info(f"Wrote {df.count()} rows to {path} (mode={mode})")
    except Exception as e:
        logger.error(f"Failed to write target at {path}: {e}")
        raise


def main():
    logger.info(f"Starting pp-agent-onboarding-v1 run — ETL_BATCH_ID={ETL_BATCH_ID}")
    try:
        ref_pp_tier = build_ref_pp_tier()
        write_target(ref_pp_tier, TGT_REF_PP_TIER, mode="append")  # SCD0: append-only

        # Use full historical + new tier codes (existing + new) for validation
        try:
            existing_tiers = spark.read.format("parquet").load(TGT_REF_PP_TIER)
            valid_tier_codes = existing_tiers.select("TIER_CD").distinct()
        except Exception:
            valid_tier_codes = ref_pp_tier.select("TIER_CD").distinct()

        dim_pp_agent = build_dim_pp_agent(valid_tier_codes)
        write_target(dim_pp_agent, TGT_DIM_PP_AGENT, mode="overwrite")

        dim_pp_certification = build_dim_pp_certification(dim_pp_agent)
        write_target(dim_pp_certification, TGT_DIM_PP_CERTIFICATION, mode="overwrite")

        logger.info("pp-agent-onboarding-v1 run completed successfully.")
    except Exception as e:
        logger.error(f"pp-agent-onboarding-v1 run failed: {e}")
        raise
    finally:
        if GLUE_ENV:
            job.commit()


if __name__ == "__main__":
    main()
```