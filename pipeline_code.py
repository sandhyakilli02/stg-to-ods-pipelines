```python
"""
ODS_CUSTOMER_ACCOUNT — SCD Type 2 Incremental Load
====================================================
Source  : STG_SALESFORCE_ACCOUNTS
Target  : ODS_CUSTOMER.ODS_CUSTOMER_ACCOUNT
Engine  : AWS Glue / EMR (PySpark + Delta Lake)
STTM ref: Salesforce CRM → ODS Customer Dimension (SCD Type 2)
"""

import sys
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DecimalType,
    TimestampType,
    BooleanType,
)
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("ods_customer_account_scd2")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration — replace S3 placeholders with environment-specific values
# ─────────────────────────────────────────────────────────────────────────────
SOURCE_PATH    = "s3://your-data-lake/staging/salesforce/accounts/"   # STG_SALESFORCE_ACCOUNTS
TARGET_PATH    = "s3://your-data-lake/ods/customer/account/"          # ODS_CUSTOMER_ACCOUNT (Delta)
REJECTION_PATH = "s3://your-data-lake/rejections/ods_customer_account/"
SOURCE_TABLE   = "STG_SALESFORCE_ACCOUNTS"

# ─────────────────────────────────────────────────────────────────────────────
# SparkSession (Glue-compatible; GlueContext is injected automatically in job mode)
# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ODS_CUSTOMER_ACCOUNT_SCD2_Load")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Import DeltaTable after session is created
from delta.tables import DeltaTable  # noqa: E402 (post-session import)


# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def utc_now() -> datetime:
    """Return the current time as a timezone-naive UTC datetime (Spark convention)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def write_rejections(df: DataFrame, batch_run_id: str, batch_run_ts: datetime,
                     reason_code: str) -> None:
    """
    STTM Business Rule: Rejection logging.
    Append rejected rows to the dead-letter store with forensic metadata.
    """
    if df.rdd.isEmpty():
        return
    logger.warning("%s: %d row(s) written to rejection log.", reason_code, df.count())
    (
        df.select(
            F.lit(batch_run_id).alias("batch_run_id"),
            F.lit(SOURCE_TABLE).alias("source_table"),
            F.col("account_id").alias("source_row_identifier"),
            F.lit(reason_code).alias("rejection_reason_code"),
            F.lit(batch_run_ts).cast(TimestampType()).alias("rejection_timestamp"),
            F.to_json(
                F.struct(
                    "account_id", "account_name", "industry",
                    "annual_revenue", "created_date", "last_modified_date",
                )
            ).alias("raw_source_payload"),
        )
        .write.mode("append")
        .format("parquet")
        .save(REJECTION_PATH)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Step 0 — Batch run context
# ─────────────────────────────────────────────────────────────────────────────
BATCH_RUN_ID = str(uuid.uuid4())
BATCH_RUN_TS = utc_now()
batch_ts_lit = F.lit(BATCH_RUN_TS).cast(TimestampType())   # reusable Spark literal

logger.info("═══ SCD2 LOAD START ═══")
logger.info("Batch run ID : %s", BATCH_RUN_ID)
logger.info("Batch run TS : %s UTC", BATCH_RUN_TS.isoformat())

# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Read source staging table with explicit schema enforcement
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 1: Reading source %s from %s", SOURCE_TABLE, SOURCE_PATH)

source_df = (
    spark.read
    .format("parquet")           # Adjust format (csv / orc / jdbc) as needed
    .option("header", "true")
    .option("inferSchema", "false")
    .load(SOURCE_PATH)
    .select(
        F.col("account_id").cast(StringType()),
        F.col("account_name").cast(StringType()),
        F.col("industry").cast(StringType()),
        F.col("annual_revenue"),              # Keep raw; cast applied after null filter
        F.col("created_date").cast(TimestampType()),
        F.col("last_modified_date").cast(TimestampType()),
    )
    # STTM — Timestamp UTC enforcement: normalise source timestamps to UTC
    .withColumn("created_date",       F.to_utc_timestamp(F.col("created_date"), "UTC"))
    .withColumn("last_modified_date", F.to_utc_timestamp(F.col("last_modified_date"), "UTC"))
)

source_count = source_df.count()
logger.info("Source row count (pre-filter): %d", source_count)

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — NULL natural-key filter and rejection logging
#           STTM Business Rule: NULL key filter — reject with reason NULL_NATURAL_KEY
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 2: NULL natural-key filter")

null_key_df = source_df.filter(F.col("account_id").isNull())
valid_df    = source_df.filter(F.col("account_id").isNotNull())

write_rejections(null_key_df, BATCH_RUN_ID, BATCH_RUN_TS, "NULL_NATURAL_KEY")

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Deduplication guard
#           STTM Business Rule: at most one record per account_id per batch;
#           duplicates → DQ alert and halt
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 3: Deduplication check")

dup_df = (
    valid_df.groupBy("account_id")
    .count()
    .filter(F.col("count") > 1)
)
if not dup_df.rdd.isEmpty():
    dup_ids = [r["account_id"] for r in dup_df.limit(20).collect()]
    logger.error(
        "DUPLICATE_NATURAL_KEY detected — batch halted. account_ids (up to 20): %s", dup_ids
    )
    raise ValueError(
        f"[{BATCH_RUN_ID}] Dedup check failed: duplicate account_id values in source batch. "
        f"account_ids (up to 20): {dup_ids}"
    )
logger.info("Dedup check passed — all account_ids are unique in this batch.")

# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Cast annual_revenue → DECIMAL(18,2); reject non-castable rows
#           STTM Business Rule: revenue precision + INVALID_REVENUE_CAST rejection
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 4: Revenue cast DECIMAL(18,2)")

valid_df = valid_df.withColumn(
    "revenue_usd",
    F.col("annual_revenue").cast(DecimalType(18, 2)),
)

# Non-null source that produced NULL after cast = bad data
invalid_cast_df = valid_df.filter(
    F.col("annual_revenue").isNotNull() & F.col("revenue_usd").isNull()
)
write_rejections(invalid_cast_df, BATCH_RUN_ID, BATCH_RUN_TS, "INVALID_REVENUE_CAST")

# Drop invalid-cast rows from further processing
valid_df = valid_df.filter(
    ~(F.col("annual_revenue").isNotNull() & F.col("revenue_usd").isNull())
)

# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Apply STTM column mapping
#           account_id → customer_id | account_name → customer_name
#           created_date → source_created_date | last_modified_date → source_last_modified_date
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 5: Applying STTM column mappings")

incoming_df = valid_df.select(
    F.col("account_id").alias("customer_id"),                       # STTM: rename
    F.col("account_name").alias("customer_name"),                   # STTM: rename
    F.col("industry"),                                              # STTM: pass-through
    F.col("revenue_usd"),                                           # STTM: cast DECIMAL(18,2)
    F.col("created_date").alias("source_created_date"),             # STTM: rename
    F.col("last_modified_date").alias("source_last_modified_date"), # STTM: rename
)
logger.info("Incoming (valid) row count after all filters: %d", incoming_df.count())

# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — Read existing ODS target (current active rows only)
#           We only compare incoming rows against is_current = TRUE rows.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 6: Reading existing ODS target from %s", TARGET_PATH)

TARGET_SCHEMA = StructType([
    StructField("customer_account_sk",        LongType(),       False),
    StructField("customer_id",                StringType(),     False),
    StructField("customer_name",              StringType(),     True),
    StructField("industry",                   StringType(),     True),
    StructField("revenue_usd",                DecimalType(18,2),True),
    StructField("source_created_date",        TimestampType(),  True),
    StructField("source_last_modified_date",  TimestampType(),  True),
    StructField("effective_start_date",       TimestampType(),  False),
    StructField("effective_end_date",         TimestampType(),  True),
    StructField("is_current",                 BooleanType(),    False),
    StructField("dw_insert_timestamp",        TimestampType(),  False),
    StructField("dw_update_timestamp",        TimestampType(),  False),
])

try:
    full_target_df = spark.read.format("delta").load(TARGET_PATH)
    existing_current_df = full_target_df.filter(F.col("is_current") == True)
    target_is_empty = False
    logger.info("Existing current-row count: %d", existing_current_df.count())
except Exception as exc:
    logger.warning("Target Delta table not found (%s) — treating as initial load.", exc)
    target_is_empty = True
    existing_current_df = spark.createDataFrame([], schema=TARGET_SCHEMA)

# ─────────────────────────────────────────────────────────────────────────────
# Step 7 — Batch idempotency guard
#           STTM Business Rule: re-running the same batch must not create duplicates.
#           A batch is identified by its BATCH_RUN_TS written into effective_start_date
#           of newly inserted current rows.
# ─────────────────────────────────────────────────────────────────────────────
if not target_is_empty:
    logger.info("Step 7: Idempotency check for batch_run_ts=%s", BATCH_RUN_TS.isoformat())
    already_run = (
        full_target_df
        .filter(
            (F.col("effective_start_date") == batch_ts_lit) &
            (F.col("is_current") == True)
        )
        .limit(1)
        .count()
    )
    if already_run > 0:
        logger.warning(
            "Idempotency: batch_run_ts=%s already present — skipping load.",
            BATCH_RUN_TS.isoformat(),
        )
        sys.exit(0)

# ─────────────────────────────────────────────────────────────────────────────
# Step 8 — Determine current max surrogate key for SK generation
# ─────────────────────────────────────────────────────────────────────────────
if not target_is_empty:
    max_sk_row = (
        full_target_df
        .agg(F.max("customer_account_sk").alias("max_sk"))
        .collect()
    )
    max_sk = int(max_sk_row[0]["max_sk"] or 0)
else:
    max_sk = 0
logger.info("Current max customer_account_sk: %d", max_sk)

# ─────────────────────────────────────────────────────────────────────────────
# Step 9 — SCD2 change detection
#           Join incoming with existing current rows; classify as NEW / CHANGED / UNCHANGED.
#           STTM: change fires on customer_name, industry, revenue_usd (NULL-safe comparison).
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 9: SCD2 change detection")

joined_df = incoming_df.join(
    existing_current_df.select(
        "customer_id",
        F.col("customer_account_sk").alias("tgt_sk"),
        F.col("customer_name").alias("tgt_customer_name"),
        F.col("industry").alias("tgt_industry"),
        F.col("revenue_usd").alias("tgt_revenue_usd"),
        F.col("effective_start_date").alias("tgt_eff_start"),
        F.col("dw_insert_timestamp").alias("tgt_dw_insert"),
    ),
    on="customer_id",
    how="left",
)

# NULL-safe equality: handles NULL↔non-NULL transitions as a tracked change
joined_df = joined_df.withColumn(
    "row_type",
    F.when(F.col("tgt_sk").isNull(), "NEW")
     .when(
         # STTM: SCD2 fires on customer_name, industry, revenue_usd changes
         ~F.col("customer_name").eqNullSafe(F.col("tgt_customer_name")) |
         ~F.col("industry").eqNullSafe(F.col("tgt_industry")) |
         ~F.col("revenue_usd").eqNullSafe(F.col("tgt_revenue_usd")),
         "CHANGED",
     )
     .otherwise("UNCHANGED"),
)

new_df       = joined_df.filter(F.col("row_type") == "NEW").cache()
changed_df   = joined_df.filter(F.col("row_type") == "CHANGED").cache()
unchanged_df = joined_df.filter(F.col("row_type") == "UNCHANGED")

new_count     = new_df.count()
changed_count = changed_df.count()
logger.info(
    "Row classification — NEW: %d | CHANGED: %d | UNCHANGED: %d",
    new_count, changed_count, unchanged_df.count(),
)

# ─────────────────────────────────────────────────────────────────────────────
# Step 10 — Build INSERT rows
#            NEW records  : first-time inserts (no prior target row)
#            CHANGED rows : successor version rows (SCD2 new active record)
#            Surrogate keys assigned via row_number() over a monotonic partition
#            to guarantee uniqueness across both sets in a single pass.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 10: Building insert rows (NEW + CHANGED successors)")

# Combine NEW and CHANGED into one frame before assigning surrogate keys
# to guarantee SK uniqueness across both sets.
new_cols     = new_df.select(
    F.col("customer_id"), F.col("customer_name"), F.col("industry"),
    F.col("revenue_usd"), F.col("source_created_date"),
    F.col("source_last_modified_date"),
)
changed_cols = changed_df.select(
    F.col("customer_id"), F.col("customer_name"), F.col("industry"),
    F.col("revenue_usd"), F.col("source_created_date"),
    F.col("source_last_modified_date"),
)
combined_inserts = new_cols.unionByName(changed_cols)

# Assign surrogate keys: row_number() over a stable order ensures uniqueness;
# offset by max_sk so existing SKs are never reused (STTM: SK uniqueness rule).
sk_window = Window.orderBy(F.monotonically_increasing_id())
all_inserts_df = (
    combined_inserts
    .withColumn(
        "customer_account_sk",
        (F.row_number().over(sk_window) + F.lit(max_sk)).cast(LongType()),
    )
    .withColumn("effective_start_date", batch_ts_lit)                         # STTM: batch TS
    .withColumn("effective_end_date",   F.lit(None).cast(TimestampType()))    # STTM: NULL = current
    .withColumn("is_current",           F.lit(True))
    .withColumn("dw_insert_timestamp",  batch_ts_lit)                         # STTM: audit column
    .withColumn("dw_update_timestamp",  batch_ts_lit)                         # STTM: audit column
)

# ─────────────────────────────────────────────────────────────────────────────
# Step 11 — Build UPDATE rows (expire prior active versions for CHANGED keys)
#            STTM: effective_end_date = successor effective_start_date − 1 ms
#            STTM: dw_update_timestamp refreshed; is_current → FALSE
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 11: Building expire rows for CHANGED customer_ids")

expire_key_df = changed_df.select("customer_id").distinct()

rows_to_expire_df = (
    existing_current_df
    .join(expire_key_df, on="customer_id", how="inner")
    # STTM: effective_end_date = successor effective_start_date − 1 ms
    .withColumn(
        "effective_end_date",
        (batch_ts_lit - F.expr("INTERVAL 1 MILLISECOND")).cast(TimestampType()),
    )
    .withColumn("is_current",          F.lit(False))
    .withColumn("dw_update_timestamp", batch_ts_lit)   # STTM: refresh on modify
    # dw_insert_timestamp is intentionally preserved from the original row
)

# ─────────────────────────────────────────────────────────────────────────────
# Step 12 — Write results
#            Initial load  : overwrite write
#            Incremental   : Delta MERGE for atomicity + idempotency
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 12: Writing to ODS target")

if target_is_empty:
    logger.info("Initial load — writing all inserts directly.")
    (
        all_inserts_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("is_current")
        .save(TARGET_PATH)
    )
else:
    delta_target = DeltaTable.forPath(spark, TARGET_PATH)

    # 12a — Expire changed rows (UPDATE is_current, effective_end_date, dw_update_ts)
    expire_count = rows_to_expire_df.count()
    if expire_count > 0:
        logger.info("Expiring %d current row(s) for CHANGED customer_ids.", expire_count)
        (
            delta_target.alias("tgt")
            .merge(
                rows_to_expire_df.alias("exp"),
                condition=(
                    "tgt.customer_account_sk = exp.customer_account_sk "
                    "AND tgt.is_current = true"
                ),
            )
            .whenMatchedUpdate(set={
                "is_current":          "exp.is_current",
                "effective_end_date":  "exp.effective_end_date",
                "dw_update_timestamp": "exp.dw_update_timestamp",
            })
            .execute()
        )

    # 12b — Insert new and successor version rows
    insert_count = all_inserts_df.count()
    if insert_count > 0:
        logger.info("Inserting %d row(s) (NEW + CHANGED successors).", insert_count)
        (
            delta_target.alias("tgt")
            .merge(
                all_inserts_df.alias("ins"),
                "tgt.customer_account_sk = ins.customer_account_sk",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# ─────────────────────────────────────────────────────────────────────────────
# Step 13 — Post-load DQ validation
#            STTM Business Rule: single active version constraint
#            At most one is_current = TRUE per customer_id; raise alert on violation.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Step 13: Post-load DQ — single active version constraint")

dq_violations = (
    spark.read.format("delta").load(TARGET_PATH)
    .filter(F.col("is_current") == True)
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

violation_count = dq_violations.count()
if violation_count > 0:
    bad_ids = [r["customer_id"] for r in dq_violations.limit(20).collect()]
    logger.error(
        "DQ ALERT — Single active version violated for %d customer_id(s): %s",
        violation_count, bad_ids,
    )
    raise RuntimeError(
        f"[{BATCH_RUN_ID}] Post-load DQ failure: {violation_count} customer_id(s) "
        f"have multiple is_current=TRUE rows. customer_ids (up to 20): {bad_ids}"
    )

logger.info("Post-load DQ passed — single active version constraint satisfied.")

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────
final_insert_count = all_inserts_df.count()
final_expire_count = rows_to_expire_df.count() if not target_is_empty else 0

logger.info(
    "═══ SCD2 LOAD COMPLETE ═══ run_id=%s | inserts=%d | expires=%d",
    BATCH_RUN_ID, final_insert_count, final_expire_count,
)
```