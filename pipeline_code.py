```python
import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger = logging.getLogger("KAN-24-plcy-detl-omnibus")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

# ---------------------------------------------------------------------------
# Glue / Spark bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Source paths (placeholders — replace with actual S3 / Glue Catalog locations)
# ---------------------------------------------------------------------------
TRADE_ASSET_STG_PATH = "s3://bucket/staging/salesiq_eventbased_trade_asset/"
TA_ACCOUNTS_PATH = "s3://bucket/reference/ta_accounts/"
SALESIQ_REF_VAL_PATH = "s3://bucket/reference/salesiq_ref_val/"
PLCY_MASTR_PATH = "s3://bucket/ods/plcy_mastr/"
PLCY_DETL_TARGET_PATH = "s3://bucket/ods/plcy_detl/"

REGULAR_SRC_CD_TRX = "STS-EXT"
REGULAR_VNDR_SRC_CD = "STS-DSP"
OMNIBUS_NSCC_MATRIX_LVL_DEFAULT = "3"


def read_source(path, fmt="parquet"):
    """Read a source dataset with basic error handling/logging."""
    try:
        logger.info(f"Reading source: {path}")
        df = spark.read.format(fmt).load(path)
        logger.info(f"Read {df.count()} rows from {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read source at {path}: {e}")
        raise


def main():
    # -----------------------------------------------------------------
    # 1. Read sources
    # -----------------------------------------------------------------
    trade_asset_df = read_source(TRADE_ASSET_STG_PATH)
    ta_accounts_df = read_source(TA_ACCOUNTS_PATH)
    salesiq_ref_val_df = read_source(SALESIQ_REF_VAL_PATH)
    plcy_mastr_df = read_source(PLCY_MASTR_PATH)

    # -----------------------------------------------------------------
    # 2. STTM Rule: Derive ROUTING_FLAG from SRC_CD_TRX / VNDR_SRC_CD
    #    REGULAR = SRC_CD_TRX = 'STS-EXT' OR VNDR_SRC_CD = 'STS-DSP'
    #    OMNIBUS = SRC_CD_TRX != 'STS-EXT' AND VNDR_SRC_CD != 'STS-DSP'
    #    (Mutually exclusive and exhaustive logical complements.)
    # -----------------------------------------------------------------
    regular_cond = (F.col("SRC_CD_TRX") == F.lit(REGULAR_SRC_CD_TRX)) | (
        F.col("VNDR_SRC_CD") == F.lit(REGULAR_VNDR_SRC_CD)
    )

    trade_asset_df = trade_asset_df.withColumn(
        "ROUTING_FLAG",
        F.when(regular_cond, F.lit("REGULAR")).otherwise(F.lit("OMNIBUS")),
    )

    # -----------------------------------------------------------------
    # 3. Lookup PLCY_MASTR_ID via existing Envision join on ACCT_NUM = PLCY_NUM
    #    (Unchanged from existing process — required FK for PLCY_DETL grain.)
    # -----------------------------------------------------------------
    plcy_mastr_envision_df = plcy_mastr_df.filter(F.col("SRC_SYS") == F.lit("ENVISION"))

    trade_asset_with_mastr_df = trade_asset_df.join(
        plcy_mastr_envision_df.select(
            F.col("PLCY_MASTR_ID"), F.col("PLCY_NUM").alias("_PLCY_NUM_LOOKUP")
        ),
        trade_asset_df["ACCT_NUM"] == F.col("_PLCY_NUM_LOOKUP"),
        "left",
    ).drop("_PLCY_NUM_LOOKUP")

    # -----------------------------------------------------------------
    # 4a. REGULAR path: CUSTDN_FIRM_NM / NSCC_MATRIX_LVL from TA_Accounts
    #     (existing Envision process, unchanged)
    # -----------------------------------------------------------------
    regular_df = trade_asset_with_mastr_df.filter(F.col("ROUTING_FLAG") == "REGULAR").join(
        ta_accounts_df.select(
            F.col("ACCT_NUM").alias("_TA_ACCT_NUM"),
            F.col("CUSTDN_FIRM_NM").alias("_TA_CUSTDN_FIRM_NM"),
            F.col("NSCC_MATRIX_LVL").alias("_TA_NSCC_MATRIX_LVL"),
        ),
        F.col("ACCT_NUM") == F.col("_TA_ACCT_NUM"),
        "left",
    ).withColumn(
        "CUSTDN_FIRM_NM", F.col("_TA_CUSTDN_FIRM_NM")
    ).withColumn(
        "NSCC_MATRIX_LVL", F.col("_TA_NSCC_MATRIX_LVL")
    ).drop("_TA_ACCT_NUM", "_TA_CUSTDN_FIRM_NM", "_TA_NSCC_MATRIX_LVL")

    # -----------------------------------------------------------------
    # 4b. OMNIBUS path: CUSTDN_FIRM_NM from SALESIQ_REF_VAL.SRC_SYS_REF_DESC
    #     via SRC_SYS_REF_CD = SRC_CD_TRX; NSCC_MATRIX_LVL hard-coded '3'.
    #     DQ check: flag records with no SALESIQ_REF_VAL match instead of
    #     silently nulling CUSTDN_FIRM_NM.
    # -----------------------------------------------------------------
    omnibus_df = trade_asset_with_mastr_df.filter(F.col("ROUTING_FLAG") == "OMNIBUS").join(
        salesiq_ref_val_df.select(
            F.col("SRC_SYS_REF_CD").alias("_REF_CD"),
            F.col("SRC_SYS_REF_DESC").alias("_REF_DESC"),
        ),
        F.col("SRC_CD_TRX") == F.col("_REF_CD"),
        "left",
    ).withColumn(
        "CUSTDN_FIRM_NM", F.col("_REF_DESC")
    ).withColumn(
        "NSCC_MATRIX_LVL", F.lit(OMNIBUS_NSCC_MATRIX_LVL_DEFAULT)
    ).withColumn(
        "DQ_FLAG_NO_REF_MATCH",
        F.when(F.col("_REF_DESC").isNull(), F.lit(True)).otherwise(F.lit(False)),
    ).drop("_REF_CD", "_REF_DESC")

    omnibus_unmatched_count = omnibus_df.filter(F.col("DQ_FLAG_NO_REF_MATCH") == True).count()
    if omnibus_unmatched_count > 0:
        logger.warning(
            f"DQ ALERT: {omnibus_unmatched_count} OMNIBUS record(s) had no "
            f"matching SRC_SYS_REF_CD in SALESIQ_REF_VAL. CUSTDN_FIRM_NM is null "
            f"for these rows; flagged via DQ_FLAG_NO_REF_MATCH for review."
        )

    regular_df = regular_df.withColumn("DQ_FLAG_NO_REF_MATCH", F.lit(False))

    # -----------------------------------------------------------------
    # 5. Union REGULAR and OMNIBUS results (mutually exclusive branches)
    # -----------------------------------------------------------------
    common_cols = regular_df.columns
    plcy_detl_result_df = regular_df.select(*common_cols).unionByName(
        omnibus_df.select(*common_cols)
    )

    # Cast target columns per data model (String)
    plcy_detl_result_df = (
        plcy_detl_result_df.withColumn("CUSTDN_FIRM_NM", F.col("CUSTDN_FIRM_NM").cast(StringType()))
        .withColumn("NSCC_MATRIX_LVL", F.col("NSCC_MATRIX_LVL").cast(StringType()))
    )

    logger.info(f"Total PLCY_DETL records processed: {plcy_detl_result_df.count()}")

    # -----------------------------------------------------------------
    # 6. Write to target (Type-1 overwrite-in-place update on PLCY_MASTR_ID)
    # -----------------------------------------------------------------
    try:
        (
            plcy_detl_result_df.write.mode("overwrite")
            .format("parquet")
            .partitionBy("ROUTING_FLAG")
            .save(PLCY_DETL_TARGET_PATH)
        )
        logger.info(f"Successfully wrote PLCY_DETL updates to {PLCY_DETL_TARGET_PATH}")
    except Exception as e:
        logger.error(f"Failed to write PLCY_DETL target: {e}")
        raise

    job.commit()


if __name__ == "__main__":
    main()
```