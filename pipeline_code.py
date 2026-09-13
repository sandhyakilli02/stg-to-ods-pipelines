```python
"""
KAN-55 | PLCY_NEW_BUSNS_IE changes for Term to Term
-------------------------------------------------------
Job:   plcy_new_busns_ie_term_to_term.py
Owner: sipinisetty@deloitte.com

Execution order
  1. Load STG_IE_DAILY_FILE  →  PLCY_NEW_BUSNS_IE  (INSERT, prod_credit = NULL)
  2. FAST_PARAM post-load UPDATE  →  prod_credit = annlzd_prem  (TERM records only)
  3. Fusion load  →  PLCY_TRX_SUMRY  (reads pre-computed prod_credit)

Target: AWS Glue / EMR  |  PySpark DataFrame API
"""

import logging
import sys
from datetime import date, datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, LongType, StringType, TimestampType
)

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "env"])
ENV = args["env"]  # e.g. "dev" | "uat" | "prod"

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark: SparkSession = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

logger.info("KAN-55 job started  env=%s", ENV)

# ---------------------------------------------------------------------------
# S3 path placeholders  (override via Glue job parameters)
# ---------------------------------------------------------------------------
S3_BASE        = f"s3://your-bucket-{ENV}"
SRC_STG_PATH   = f"{S3_BASE}/life-ie-sys/stg_ie_daily_file/"
TGT_IE_PATH    = f"{S3_BASE}/ods/plcy_new_busns_ie/"
TGT_TRX_PATH   = f"{S3_BASE}/ods/plcy_trx_sumry/"
TGT_ERR_PATH   = f"{S3_BASE}/ods/plcy_new_busns_ie_errors/"
FAST_PARAM_PATH = f"{S3_BASE}/ods/fast_param/"

PROCESSING_DATE = date.today()

# ---------------------------------------------------------------------------
# Helper: monotonically increasing surrogate key (Glue-safe)
# ---------------------------------------------------------------------------
def add_surrogate_key(df: DataFrame, col_name: str) -> DataFrame:
    """Add an auto-increment-style surrogate PK via monotonically_increasing_id."""
    return df.withColumn(col_name, F.monotonically_increasing_id())


# ===========================================================================
# STEP 1 — Read source: STG_IE_DAILY_FILE
# ===========================================================================
logger.info("STEP 1: Reading STG_IE_DAILY_FILE from %s", SRC_STG_PATH)

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(SRC_STG_PATH)
)

logger.info("Source row count: %d", raw_df.count())


# ===========================================================================
# STEP 2 — Transform & validate → PLCY_NEW_BUSNS_IE
# ===========================================================================
logger.info("STEP 2: Applying STTM transformations for PLCY_NEW_BUSNS_IE")

# -- Apply column-level transformations per STTM --
transformed_df = (
    raw_df
    # STTM: TRIM mandatory string fields
    .withColumn("plcy_num",     F.trim(F.col("plcy_num")))
    .withColumn("repl_plcy_num", F.trim(F.col("repl_plcy_num")))
    # STTM: UPPER(TRIM) — must equal 'TERM' for term-to-term scope
    .withColumn("plcy_typ_cd",  F.upper(F.trim(F.col("plcy_typ_cd"))))
    .withColumn("ie_typ_cd",    F.upper(F.trim(F.col("ie_typ_cd"))))
    # STTM: CAST dates (YYYY-MM-DD); issue_dt nullable
    .withColumn("new_busns_dt", F.to_date(F.col("new_busns_dt"), "yyyy-MM-dd"))
    .withColumn("issue_dt",     F.to_date(F.col("issue_dt"),     "yyyy-MM-dd"))
    # STTM: CAST numeric fields
    .withColumn("face_amt",     F.col("face_amt").cast(DecimalType(18, 2)))
    .withColumn("annlzd_prem",  F.col("annlzd_prem").cast(DecimalType(18, 2)))
    .withColumn("term_dur_mths", F.col("term_dur_mths").cast(IntegerType()))
    # STTM: src_sys_cd — COALESCE(UPPER(TRIM()), 'LIFE_IE')
    .withColumn("src_sys_cd",   F.coalesce(F.upper(F.trim(F.col("src_sys_cd"))), F.lit("LIFE_IE")))
    # STTM: NEW COLUMN prod_credit — NULL on initial INSERT; set by FAST_PARAM post-load
    .withColumn("prod_credit",        F.lit(None).cast(DecimalType(18, 2)))
    .withColumn("prod_credit_calc_dt", F.lit(None).cast(TimestampType()))
    # STTM: Audit timestamps — load_dt set at INSERT, upd_dt NULL on initial INSERT
    .withColumn("load_dt", F.current_timestamp())
    .withColumn("upd_dt",  F.lit(None).cast(TimestampType()))
)

# -- Data Quality: reject records missing mandatory fields --
mandatory_fields = ["plcy_num", "repl_plcy_num", "plcy_typ_cd", "new_busns_dt", "face_amt", "annlzd_prem"]
null_condition   = F.lit(False)
for field in mandatory_fields:
    null_condition = null_condition | F.col(field).isNull() | (F.col(field).cast(StringType()) == "")

# STTM BR: plcy_typ_cd must be 'TERM'; reject unexpected values with ERR_PLCY_TYP_CD_INVALID
dq_reject_condition = null_condition | (F.col("plcy_typ_cd") != "TERM")

reject_df = (
    transformed_df
    .filter(dq_reject_condition)
    .withColumn("reject_reason", F.when(
        F.col("plcy_typ_cd") != "TERM", F.lit("ERR_PLCY_TYP_CD_INVALID")
    ).otherwise(F.lit("ERR_MANDATORY_FIELD_NULL")))
    .withColumn("reject_dt", F.current_timestamp())
)

clean_df = transformed_df.filter(~dq_reject_condition)

reject_count = reject_df.count()
clean_count  = clean_df.count()
logger.info("DQ results — clean: %d  |  rejected: %d", clean_count, reject_count)

if reject_count > 0:
    logger.warning("Routing %d rejected records to error table: %s", reject_count, TGT_ERR_PATH)
    (
        reject_df.write
        .mode("append")
        .partitionBy("reject_reason")
        .parquet(TGT_ERR_PATH)
    )

# STTM BR: face_amt and annlzd_prem must be positive — log DQ warning
negative_numeric = clean_df.filter((F.col("face_amt") <= 0) | (F.col("annlzd_prem") <= 0))
neg_count = negative_numeric.count()
if neg_count > 0:
    logger.warning("DQ WARNING: %d records have zero/negative face_amt or annlzd_prem — review required", neg_count)

# -- Deduplication: skip plcy_num + new_busns_dt duplicates within today's file --
# STTM BR: confirm dedup strategy with business; using keep-first as baseline
clean_dedup_df = clean_df.dropDuplicates(["plcy_num", "new_busns_dt"])
dedup_dropped  = clean_count - clean_dedup_df.count()
if dedup_dropped > 0:
    logger.warning("Deduplication dropped %d duplicate plcy_num+new_busns_dt rows", dedup_dropped)

# -- Add surrogate PK (plcy_ie_id) --
ie_insert_df = add_surrogate_key(clean_dedup_df, "plcy_ie_id")

# -- Final column selection / ordering for PLCY_NEW_BUSNS_IE --
ie_final_df = ie_insert_df.select(
    F.col("plcy_ie_id").cast(LongType()),
    F.col("plcy_num").cast(StringType()),
    F.col("repl_plcy_num").cast(StringType()),
    F.col("plcy_typ_cd").cast(StringType()),
    F.col("ie_typ_cd").cast(StringType()),
    F.col("new_busns_dt"),
    F.col("issue_dt"),
    F.col("face_amt"),
    F.col("annlzd_prem"),
    F.col("term_dur_mths"),
    F.col("prod_credit"),           # NULL on initial INSERT
    F.col("prod_credit_calc_dt"),   # NULL until FAST_PARAM runs
    F.col("src_sys_cd").cast(StringType()),
    F.col("load_dt"),
    F.col("upd_dt"),
)

logger.info("Writing %d records to PLCY_NEW_BUSNS_IE: %s", ie_final_df.count(), TGT_IE_PATH)
(
    ie_final_df.write
    .mode("append")
    .partitionBy("new_busns_dt")
    .parquet(TGT_IE_PATH)
)
logger.info("STEP 2 complete — PLCY_NEW_BUSNS_IE INSERT done")


# ===========================================================================
# STEP 3 — FAST_PARAM post-load UPDATE: compute prod_credit for TERM records
#
# STTM: prod_credit = CAST(annlzd_prem AS DECIMAL(18,2))
# Scope: plcy_typ_cd = 'TERM' AND prod_credit IS NULL AND load_dt >= today
# Idempotency: re-runnable; already-credited rows are not touched
# NOTE: Confirm exact formula (multiplier / rate table) with business owner
# ===========================================================================
logger.info("STEP 3: FAST_PARAM post-load UPDATE — computing prod_credit for TERM records")

# Read back today's PLCY_NEW_BUSNS_IE partition (just inserted)
ie_today_df = (
    spark.read
    .parquet(TGT_IE_PATH)
    .filter(
        (F.col("plcy_typ_cd") == "TERM")          # STTM: TERM records only
        & F.col("prod_credit").isNull()            # STTM: idempotency guard
        & (F.col("load_dt") >= F.lit(PROCESSING_DATE).cast("timestamp"))  # current-day scope
    )
)

eligible_count = ie_today_df.count()
logger.info("FAST_PARAM eligible records (TERM, prod_credit IS NULL, today): %d", eligible_count)

if eligible_count > 0:
    # STTM: prod_credit = annlzd_prem  (baseline — confirm multiplier with business)
    ie_credited_df = (
        ie_today_df
        .withColumn("prod_credit",         F.col("annlzd_prem").cast(DecimalType(18, 2)))
        .withColumn("prod_credit_calc_dt", F.current_timestamp())  # STTM: co-populated with prod_credit
        .withColumn("upd_dt",              F.current_timestamp())
    )

    # Overwrite the today partition with credited values (upsert via partition overwrite)
    (
        ie_credited_df.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("new_busns_dt")
        .parquet(TGT_IE_PATH)
    )
    logger.info("STEP 3 complete — prod_credit set on %d TERM records", eligible_count)
else:
    logger.info("STEP 3: No eligible TERM records for FAST_PARAM UPDATE today")


# ===========================================================================
# STEP 4 — Fusion load: PLCY_TRX_SUMRY
#
# STTM: Read PLCY_NEW_BUSNS_IE (prod_credit pre-computed); build summary rows
# Dependency: must run AFTER STEP 3 completes successfully
# ===========================================================================
logger.info("STEP 4: Building PLCY_TRX_SUMRY from PLCY_NEW_BUSNS_IE (fusion load)")

# Read updated PLCY_NEW_BUSNS_IE (with prod_credit populated for TERM records)
ie_for_fusion_df = (
    spark.read
    .parquet(TGT_IE_PATH)
    .filter(
        F.col("load_dt") >= F.lit(PROCESSING_DATE).cast("timestamp")  # today's records only
    )
)

# STTM: Build PLCY_TRX_SUMRY rows
trx_df = (
    ie_for_fusion_df
    # STTM: trx_sumry_id — surrogate PK (added below)
    # STTM: plcy_ie_id — direct FK copy
    # STTM: plcy_num — denormalized direct copy
    # STTM: trx_dt = new_busns_dt
    .withColumn("trx_dt",             F.col("new_busns_dt"))
    # STTM: trx_typ_cd — hardcoded constant for IE new business
    .withColumn("trx_typ_cd",         F.lit("NEW_BUSNS_IE").cast(StringType()))
    # STTM: prod_credit — direct copy; pre-computed; NULL propagated if FAST_PARAM failed
    # (no transformation needed — column already named prod_credit)
    # STTM: fusion_load_dt — set at commit time
    .withColumn("fusion_load_dt",     F.current_timestamp())
    # STTM: fusion_load_stts_cd — initial value 'PENDING'
    .withColumn("fusion_load_stts_cd", F.lit("PENDING").cast(StringType()))
    # STTM: load_dt — INSERT timestamp
    .withColumn("load_dt",            F.current_timestamp())
    # STTM: upd_dt — NULL on initial INSERT
    .withColumn("upd_dt",             F.lit(None).cast(TimestampType()))
)

# Add surrogate PK for PLCY_TRX_SUMRY
trx_final_df = add_surrogate_key(trx_df, "trx_sumry_id")

trx_final_df = trx_final_df.select(
    F.col("trx_sumry_id").cast(LongType()),
    F.col("plcy_ie_id").cast(LongType()),
    F.col("plcy_num").cast(StringType()),
    F.col("trx_dt"),
    F.col("trx_typ_cd"),
    F.col("prod_credit"),            # STTM: pre-computed; no recalculation at fusion
    F.col("fusion_load_dt"),
    F.col("fusion_load_stts_cd"),
    F.col("load_dt"),
    F.col("upd_dt"),
)

# STTM BR: warn if any prod_credit is NULL at fusion time (FAST_PARAM may have failed)
null_prod_credit = trx_final_df.filter(
    (F.col("trx_typ_cd") == "NEW_BUSNS_IE") & F.col("prod_credit").isNull()
).count()
if null_prod_credit > 0:
    logger.warning(
        "ALERT: %d PLCY_TRX_SUMRY rows have NULL prod_credit — "
        "FAST_PARAM UPDATE may have failed. Halt fusion or re-attempt after FAST_PARAM reruns.",
        null_prod_credit,
    )

logger.info("Writing %d records to PLCY_TRX_SUMRY: %s", trx_final_df.count(), TGT_TRX_PATH)
(
    trx_final_df.write
    .mode("append")
    .partitionBy("trx_dt")
    .parquet(TGT_TRX_PATH)
)

# STTM BR: update fusion_load_stts_cd from PENDING → LOADED on success
logger.info("Updating fusion_load_stts_cd PENDING → LOADED for committed rows")
trx_loaded_df = (
    spark.read
    .parquet(TGT_TRX_PATH)
    .filter(
        (F.col("fusion_load_stts_cd") == "PENDING")
        & (F.col("load_dt") >= F.lit(PROCESSING_DATE).cast("timestamp"))
    )
    .withColumn("fusion_load_stts_cd", F.lit("LOADED"))
    .withColumn("upd_dt",              F.current_timestamp())
)
(
    trx_loaded_df.write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("trx_dt")
    .parquet(TGT_TRX_PATH)
)
logger.info("STEP 4 complete — PLCY_TRX_SUMRY fusion load done")


# ===========================================================================
# STEP 5 — FAST_PARAM deploy-time INSERT (one-time; skip if record exists)
#
# STTM: Manual config record for the prod_credit calculation query.
# Run once at deployment. Subsequent runs are idempotent (check param_cd).
# ===========================================================================
logger.info("STEP 5: Checking / inserting FAST_PARAM record")

try:
    existing_fp = (
        spark.read
        .parquet(FAST_PARAM_PATH)
        .filter(F.col("param_cd") == "PLCY_NEW_BUSNS_IE_PROD_CREDIT")
    )
    already_exists = existing_fp.count() > 0
except Exception:
    already_exists = False  # table doesn't exist yet

if not already_exists:
    logger.info("Inserting FAST_PARAM record for PLCY_NEW_BUSNS_IE_PROD_CREDIT")
    # STTM: qry_txt — SQL executed by fast-param processor after daily IE file load
    fast_param_qry = (
        "UPDATE PLCY_NEW_BUSNS_IE "
        "SET prod_credit = CAST(annlzd_prem AS DECIMAL(18,2)), "
        "    prod_credit_calc_dt = CURRENT_TIMESTAMP, "
        "    upd_dt = CURRENT_TIMESTAMP "
        "WHERE plcy_typ_cd = 'TERM' "
        "  AND prod_credit IS NULL "
        "  AND load_dt >= TRUNC(SYSDATE)"
    )
    go_live_date = date.today().isoformat()  # Replace with actual go-live date at deployment

    fp_data = [(
        1,                                                                   # param_id (sequence)
        "PLCY_NEW_BUSNS_IE_PROD_CREDIT",                                    # param_cd
        "Production Credit Calculation - Term-to-Term Internal Exchange (PLCY_NEW_BUSNS_IE)",  # param_nm
        fast_param_qry,                                                      # qry_txt
        "Y",                                                                 # actv_ind
        go_live_date,                                                        # eff_dt
        None,                                                                # exp_dt (indefinitely active)
        "DATACRAFT_IMPL",                                                    # crt_by
        datetime.utcnow().isoformat(),                                       # crt_dt
        None,                                                                # upd_by
        None,                                                                # upd_dt
    )]

    fp_schema = (
        "param_id INT, param_cd STRING, param_nm STRING, qry_txt STRING, "
        "actv_ind STRING, eff_dt STRING, exp_dt STRING, crt_by STRING, "
        "crt_dt STRING, upd_by STRING, upd_dt STRING"
    )
    fp_df = spark.createDataFrame(fp_data, schema=fp_schema)
    (
        fp_df.write
        .mode("append")
        .parquet(FAST_PARAM_PATH)
    )
    logger.info("STEP 5 complete — FAST_PARAM record inserted")
else:
    logger.info("STEP 5 skipped — FAST_PARAM record already exists (idempotent)")


# ---------------------------------------------------------------------------
# Job complete
# ---------------------------------------------------------------------------
logger.info(
    "KAN-55 job completed successfully | env=%s | processing_date=%s | "
    "ie_inserted=%d | trx_loaded=%d | rejected=%d",
    ENV, PROCESSING_DATE, clean_dedup_df.count(), trx_final_df.count(), reject_count,
)
job.commit()
```