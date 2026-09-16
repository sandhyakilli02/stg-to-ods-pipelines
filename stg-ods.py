"""Common job for CustomerStaging to CustomerODS SCD Type 2 load"""

import sys
import json
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("CustomerStaging_To_ODS_SCD2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Pipeline timestamp – captured once and reused for every audit column
# ---------------------------------------------------------------------------
PIPELINE_TS = F.current_timestamp()

# ---------------------------------------------------------------------------
# Source-system literal used in src_sys_cd when staging row has no value
# ---------------------------------------------------------------------------
DEFAULT_SRC_SYS = "CRM"

# ---------------------------------------------------------------------------
# SCD2 hash columns – must match STTM row_hash definition
# ---------------------------------------------------------------------------
HASH_COLS = [
    "first_name", "last_name", "email", "phone",
    "address_line1", "address_line2", "city", "state",
    "zip_code", "country", "date_of_birth", "customer_type", "status",
]


# ---------------------------------------------------------------------------
# Helper: compute SHA-256 row_hash over the HASH_COLS
# ---------------------------------------------------------------------------
def compute_row_hash(df):
    concat_expr = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in HASH_COLS],
    )
    return df.withColumn("row_hash", F.sha2(concat_expr, 256))


# ---------------------------------------------------------------------------
# Helper: open a batch control row (status = RUNNING)
# ---------------------------------------------------------------------------
def open_batch(etl_df, src_sys_cd, records_extracted):
    """
    Derives batch_id as max(batch_id)+1 from ETLBatchControl.
    Returns (batch_id, updated_etl_df).
    """
    max_id_row = etl_df.agg(F.max("batch_id").alias("max_id")).collect()[0]
    batch_id = int(max_id_row["max_id"] or 0) + 1

    now = datetime.utcnow()
    new_row = spark.createDataFrame(
        [(
            batch_id,
            "CustomerStaging_To_ODS",
            src_sys_cd,
            "RUNNING",
            now,          # batch_start_dt
            None,         # batch_end_dt
            records_extracted,
            0,            # records_inserted
            0,            # records_updated
            0,            # records_rejected
            None,         # error_message
            now,          # created_dt
            None,         # updated_dt
        )],
        schema=etl_df.schema,
    )
    updated_etl_df = etl_df.union(new_row)
    return batch_id, updated_etl_df


# ---------------------------------------------------------------------------
# Helper: close a batch control row
# ---------------------------------------------------------------------------
def close_batch(etl_df, batch_id, status, inserted, updated, rejected, error_msg=None):
    now = datetime.utcnow()
    return etl_df.withColumn(
        "batch_status",
        F.when(F.col("batch_id") == batch_id, F.lit(status)).otherwise(F.col("batch_status")),
    ).withColumn(
        "batch_end_dt",
        F.when(F.col("batch_id") == batch_id, F.lit(now)).otherwise(F.col("batch_end_dt")),
    ).withColumn(
        "records_inserted",
        F.when(F.col("batch_id") == batch_id, F.lit(inserted)).otherwise(F.col("records_inserted")),
    ).withColumn(
        "records_updated",
        F.when(F.col("batch_id") == batch_id, F.lit(updated)).otherwise(F.col("records_updated")),
    ).withColumn(
        "records_rejected",
        F.when(F.col("batch_id") == batch_id, F.lit(rejected)).otherwise(F.col("records_rejected")),
    ).withColumn(
        "error_message",
        F.when(F.col("batch_id") == batch_id, F.lit(error_msg)).otherwise(F.col("error_message")),
    ).withColumn(
        "updated_dt",
        F.when(F.col("batch_id") == batch_id, F.lit(now)).otherwise(F.col("updated_dt")),
    )


# ---------------------------------------------------------------------------
# Read tables  (replace the catalog names / paths as needed for your env)
# ---------------------------------------------------------------------------
def read_table(name):
    return spark.table(name)


def write_table(df, name, mode="overwrite"):
    df.write.mode(mode).saveAsTable(name)


# ---------------------------------------------------------------------------
# Main SCD Type 2 transformation
# ---------------------------------------------------------------------------
def run():
    records_inserted = 0
    records_updated = 0
    records_rejected = 0
    batch_id = None
    error_msg = None

    try:
        # ── 1. Read source and current ODS ──────────────────────────────────
        stg_raw = read_table("CustomerStaging")
        ods_raw = read_table("CustomerODS")
        etl_raw = read_table("ETLBatchControl")

        records_extracted = stg_raw.count()
        src_sys_cd = (
            stg_raw.select("src_sys_cd")
            .dropna()
            .first()
        )
        src_sys_cd = src_sys_cd["src_sys_cd"] if src_sys_cd else DEFAULT_SRC_SYS

        # ── 2. Open ETLBatchControl row ──────────────────────────────────────
        batch_id, etl_df = open_batch(etl_raw, src_sys_cd, records_extracted)
        write_table(etl_df, "ETLBatchControl")
        print(f"Batch {batch_id} opened — {records_extracted} staging records.")

        # ── 3. Validate: reject rows missing mandatory NOT-NULL columns ──────
        reject_filter = (
            F.col("customer_id").isNull()
            | F.col("src_sys_cd").isNull()
            | F.col("rec_stat_cd").isNull()
            | F.col("load_dt").isNull()
            | F.col("batch_id").isNull()
        )
        stg_rejected = stg_raw.filter(reject_filter)
        stg_valid = stg_raw.filter(~reject_filter)

        records_rejected = stg_rejected.count()
        if records_rejected:
            print(f"WARNING: {records_rejected} staging rows rejected (NULL on NOT-NULL columns).")

        # ── 4. Compute row_hash on staging if not already present ────────────
        stg = compute_row_hash(stg_valid) if "row_hash" not in stg_valid.columns else stg_valid

        # ── 5. Separate by staging rec_stat_cd ──────────────────────────────
        stg_deletes = stg.filter(F.col("rec_stat_cd") == "D")
        stg_upserts = stg.filter(F.col("rec_stat_cd") != "D")   # N or U

        # ── 6. Current ODS rows (is_current = 'Y') ──────────────────────────
        ods_current = ods_raw.filter(F.col("is_current") == "Y").alias("ods")

        # ── 7. Detect changed rows (hash mismatch) ──────────────────────────
        stg_upserts_alias = stg_upserts.alias("stg")

        joined = stg_upserts_alias.join(
            ods_current.select("customer_id", "row_hash", "ods_customer_sk"),
            on="customer_id",
            how="left",
        )

        # New: no current ODS row exists for customer_id
        new_rows = joined.filter(F.col("ods.row_hash").isNull())
        # Changed: current ODS row exists but hash differs
        changed_rows = joined.filter(
            F.col("ods.row_hash").isNotNull()
            & (F.col("stg.row_hash") != F.col("ods.row_hash"))
        )
        # Unchanged: hash matches → skip
        unchanged_rows = joined.filter(
            F.col("ods.row_hash").isNotNull()
            & (F.col("stg.row_hash") == F.col("ods.row_hash"))
        )
        print(f"New: {new_rows.count()}, Changed: {changed_rows.count()}, "
              f"Unchanged: {unchanged_rows.count()}, Deletes: {stg_deletes.count()}")

        # ── 8. Determine max ods_customer_sk for surrogate key generation ───
        max_sk_row = ods_raw.agg(F.max("ods_customer_sk").alias("max_sk")).collect()[0]
        max_sk = int(max_sk_row["max_sk"] or 0)

        # ── 9. Build NEW-version rows (new inserts) ──────────────────────────
        def build_new_version(src_df, sk_offset_col):
            return src_df.select(
                (F.lit(max_sk).cast(LongType()) + sk_offset_col).alias("ods_customer_sk"),
                F.col("stg.customer_id"),
                F.col("stg.first_name"),
                F.col("stg.last_name"),
                F.col("stg.email"),
                F.col("stg.phone"),
                F.col("stg.address_line1"),
                F.col("stg.address_line2"),
                F.col("stg.city"),
                F.col("stg.state"),
                F.col("stg.zip_code"),
                F.col("stg.country"),
                F.col("stg.date_of_birth"),
                F.col("stg.customer_type"),
                F.col("stg.status"),
                F.col("stg.row_hash"),
                PIPELINE_TS.alias("eff_start_dt"),
                F.lit(None).cast("timestamp").alias("eff_end_dt"),
                F.lit("Y").alias("is_current"),
                F.col("stg.src_sys_cd"),
                F.when(F.col("stg.rec_stat_cd").isin("N", "U"), F.lit("A"))
                 .otherwise(F.lit("A")).alias("rec_stat_cd"),
                PIPELINE_TS.alias("load_dt"),
                F.coalesce(F.col("stg.upd_dt"), PIPELINE_TS).alias("upd_dt"),
                F.lit(batch_id).cast(LongType()).alias("batch_id"),
            )

        new_rows_with_offset = new_rows.withColumn(
            "_sk_offset", F.monotonically_increasing_id() + 1
        )
        changed_rows_with_offset = changed_rows.withColumn(
            "_sk_offset",
            F.monotonically_increasing_id() + 1 + new_rows.count()
        )

        new_ods_from_new = build_new_version(new_rows_with_offset, F.col("_sk_offset"))
        new_ods_from_changed = build_new_version(changed_rows_with_offset, F.col("_sk_offset"))

        # ── 10. Expire rows that have changed (set eff_end_dt, is_current='N') ─
        changed_customer_ids = changed_rows.select(F.col("stg.customer_id").alias("customer_id"))
        expire_ts = PIPELINE_TS - F.expr("INTERVAL 1 SECOND")

        ods_expired = ods_raw.join(
            changed_customer_ids, on="customer_id", how="left_semi"
        ).filter(F.col("is_current") == "Y").withColumn(
            "eff_end_dt", expire_ts
        ).withColumn(
            "is_current", F.lit("N")
        ).withColumn(
            "upd_dt", PIPELINE_TS
        ).withColumn(
            "batch_id", F.lit(batch_id).cast(LongType())
        )

        # ── 11. Handle logical deletes ───────────────────────────────────────
        delete_customer_ids = stg_deletes.select("customer_id")

        ods_logically_deleted = ods_raw.join(
            delete_customer_ids, on="customer_id", how="left_semi"
        ).filter(F.col("is_current") == "Y").withColumn(
            "eff_end_dt", PIPELINE_TS
        ).withColumn(
            "is_current", F.lit("N")
        ).withColumn(
            "rec_stat_cd", F.lit("D")
        ).withColumn(
            "upd_dt", PIPELINE_TS
        ).withColumn(
            "batch_id", F.lit(batch_id).cast(LongType())
        )

        # ── 12. Assemble full new CustomerODS ───────────────────────────────
        all_changed_or_deleted_ids = (
            changed_customer_ids.union(delete_customer_ids).distinct()
        )
        ods_untouched = ods_raw.join(
            all_changed_or_deleted_ids, on="customer_id", how="left_anti"
        )

        customer_ods_final = (
            ods_untouched
            .union(ods_expired)
            .union(ods_logically_deleted)
            .union(new_ods_from_new)
            .union(new_ods_from_changed)
        )

        # ── 13. Count outcomes ───────────────────────────────────────────────
        records_inserted = new_ods_from_new.count() + new_ods_from_changed.count()
        records_updated = changed_rows.count()

        # ── 14. Write CustomerODS ────────────────────────────────────────────
        write_table(customer_ods_final, "CustomerODS")
        print(f"CustomerODS written — inserted: {records_inserted}, "
              f"updated (SCD2 cycles): {records_updated}.")

        # ── 15. Close batch SUCCESS ──────────────────────────────────────────
        etl_closed = close_batch(
            etl_df, batch_id, "SUCCESS" if records_rejected == 0 else "PARTIAL",
            records_inserted, records_updated, records_rejected,
        )
        write_table(etl_closed, "ETLBatchControl")
        print(f"Batch {batch_id} closed.")

    except Exception as exc:
        error_msg = (traceback.format_exc())[:2000]
        print(f"PIPELINE FAILED: {error_msg}")
        if batch_id is not None:
            try:
                etl_err = close_batch(
                    etl_df if "etl_df" in dir() else read_table("ETLBatchControl"),
                    batch_id, "FAILED",
                    records_inserted, records_updated, records_rejected, error_msg,
                )
                write_table(etl_err, "ETLBatchControl")
            except Exception as inner:
                print(f"Could not update ETLBatchControl on failure: {inner}")
        raise

    finally:
        spark.stop()


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()
