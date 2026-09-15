from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, TimestampType
from delta.tables import DeltaTable
import traceback as tb


# â”€â”€ Constants â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
PIPELINE_NAME  = "salesiq_customer_stg_to_ods"
SOURCE_SYSTEM  = "SalesIQ"
TARGET_TABLE   = "ODS_CUSTOMER"
CREATED_BY     = "<etl-service-account>"
VALID_STATUSES = ("Active", "Inactive", "Suspended")


# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _next_batch_id(spark: SparkSession) -> int:
    """Return max(batch_id) + 1, or 1 if the control table is empty/missing."""
    try:
        row = (
            spark.table("ETLBatchControl")
            .agg(F.max("batch_id").alias("m"))
            .collect()[0]
        )
        return int(row["m"]) + 1 if row["m"] is not None else 1
    except Exception:
        return 1


def _ods_payload(src_alias: str) -> list:
    """
    Column list for inserting a new ODS row from a joined DataFrame.
    ods_customer_sk is intentionally excluded; it is added after the
    new-inserts / SCD-update union via monotonically_increasing_id().
    """
    a = src_alias
    return [
        F.col(f"{a}.customer_id"),
        F.col(f"{a}.first_name"),
        F.col(f"{a}.last_name"),
        F.col(f"{a}.email"),
        F.col(f"{a}.phone"),
        F.col(f"{a}.address_line1"),
        F.col(f"{a}.address_line2"),
        F.col(f"{a}.city"),
        F.col(f"{a}.state"),
        F.col(f"{a}.postal_code"),
        F.col(f"{a}.country"),
        F.col(f"{a}.customer_segment"),
        F.col(f"{a}.account_status"),
        F.col(f"{a}.credit_limit"),
        F.col(f"{a}.annual_revenue"),
        F.col(f"{a}.industry"),
        F.col(f"{a}.source_created_date"),
        F.col(f"{a}.source_modified_date"),
        F.col(f"{a}.source_system"),
        F.col(f"{a}.row_hash"),
    ]


# â”€â”€ Main pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def main() -> None:
    spark = (
        SparkSession.builder
        .appName(PIPELINE_NAME)
        .enableHiveSupport()
        .getOrCreate()
    )

    records_read      = 0
    records_inserted  = 0
    records_updated   = 0
    records_unchanged = 0
    records_rejected  = 0
    batch_status      = "RUNNING"
    error_message: Optional[str] = None
    batch_id: Optional[int] = None

    stg_all:     Optional[DataFrame] = None
    ods_current: Optional[DataFrame] = None

    try:
        # â”€â”€ 1. Open ETL batch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        batch_id = _next_batch_id(spark)

        (
            spark.createDataFrame(
                [(
                    batch_id, PIPELINE_NAME, SOURCE_SYSTEM, TARGET_TABLE,
                    None, None, "RUNNING",
                    None, None, None, None, None, None,
                    CREATED_BY, None, None,
                )],
                schema=(
                    "batch_id LONG, pipeline_name STRING, source_system STRING, "
                    "target_table STRING, batch_start_time TIMESTAMP, "
                    "batch_end_time TIMESTAMP, batch_status STRING, "
                    "records_read LONG, records_inserted LONG, records_updated LONG, "
                    "records_unchanged LONG, records_rejected LONG, "
                    "error_message STRING, created_by STRING, "
                    "created_timestamp TIMESTAMP, updated_timestamp TIMESTAMP"
                ),
            )
            .select(
                "batch_id", "pipeline_name", "source_system", "target_table",
                F.current_timestamp().alias("batch_start_time"),
                F.lit(None).cast(TimestampType()).alias("batch_end_time"),
                "batch_status",
                F.lit(None).cast(LongType()).alias("records_read"),
                F.lit(None).cast(LongType()).alias("records_inserted"),
                F.lit(None).cast(LongType()).alias("records_updated"),
                F.lit(None).cast(LongType()).alias("records_unchanged"),
                F.lit(None).cast(LongType()).alias("records_rejected"),
                F.lit(None).cast("string").alias("error_message"),
                "created_by",
                F.current_timestamp().alias("created_timestamp"),
                F.lit(None).cast(TimestampType()).alias("updated_timestamp"),
            )
            .write.format("delta").mode("append").saveAsTable("ETLBatchControl")
        )

        # â”€â”€ 2. Read unprocessed staging rows â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        stg_all = (
            spark.table("STG_CUSTOMER")
            .filter(F.col("is_processed") == False)
            .cache()
        )
        records_read = stg_all.count()

        # â”€â”€ 3. Data-quality gate â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        is_valid_cond = (
            F.col("customer_id").isNotNull()
            & F.col("account_status").isin(*VALID_STATUSES)
            & F.col("source_system").isNotNull()
        )
        valid_df      = stg_all.filter(is_valid_cond)
        reject_stg_df = stg_all.filter(~is_valid_cond)

        reject_reason_col = (
            F.when(
                F.col("customer_id").isNull(),
                F.lit("NULL business key: customer_id is required and cannot be null"),
            )
            .when(
                ~F.col("account_status").isin(*VALID_STATUSES),
                F.concat_ws(
                    "",
                    F.lit("Invalid account_status value: "),
                    F.coalesce(F.col("account_status"), F.lit("<null>")),
                    F.lit(". Allowed values: Active, Inactive, Suspended"),
                ),
            )
            .when(
                F.col("source_system").isNull(),
                F.lit("NULL source_system: source_system is required and cannot be null"),
            )
            .otherwise(F.lit("Unspecified DQ validation failure â€” see reject_rule_code"))
        )

        reject_rule_col = (
            F.when(F.col("customer_id").isNull(),
                   F.lit("DQ001_NULL_CUSTOMER_ID"))
            .when(~F.col("account_status").isin(*VALID_STATUSES),
                  F.lit("DQ002_INVALID_ACCOUNT_STATUS"))
            .when(F.col("source_system").isNull(),
                  F.lit("DQ003_NULL_SOURCE_SYSTEM"))
            .otherwise(F.lit("DQ999_UNKNOWN_VALIDATION_FAILURE"))
        )

        raw_record_col = F.to_json(F.struct(
            "stg_customer_sk", "customer_id", "first_name", "last_name", "email",
            "phone", "address_line1", "address_line2", "city", "state",
            "postal_code", "country", "customer_segment", "account_status",
            "credit_limit", "annual_revenue", "industry",
            "source_created_date", "source_modified_date", "source_system",
            "row_hash", "batch_id", "load_timestamp", "is_processed",
        ))

        (
            reject_stg_df.select(
                F.monotonically_increasing_id().alias("reject_id"),
                F.col("stg_customer_sk"),
                F.col("customer_id"),
                F.lit(batch_id).cast(LongType()).alias("batch_id"),
                reject_reason_col.alias("reject_reason"),
                reject_rule_col.alias("reject_rule_code"),
                raw_record_col.alias("raw_record"),
                F.current_timestamp().alias("reject_timestamp"),
            )
            .write.format("delta").mode("append").saveAsTable("STG_CUSTOMER_REJECT")
        )
        records_rejected = reject_stg_df.count()

        # â”€â”€ 4. SCD Type 2 logic â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        ods_current = (
            spark.table("ODS_CUSTOMER")
            .filter(F.col("is_current") == True)
            .select("customer_id", "ods_customer_sk", "row_hash")
            .cache()
        )

        # Left-join valid staging to current ODS snapshot
        joined = valid_df.alias("stg").join(
            ods_current.alias("ods"),
            F.col("stg.customer_id") == F.col("ods.customer_id"),
            how="left",
        )

        # â”€â”€ 4a. NEW records â€” no matching ODS row â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        new_rows_df = joined.filter(F.col("ods.customer_id").isNull()).select(
            *_ods_payload("stg"),
            F.col("stg.load_timestamp").alias("effective_start_date"),
            F.lit(None).cast(TimestampType()).alias("effective_end_date"),
            F.lit(True).alias("is_current"),
            F.lit(batch_id).cast(LongType()).alias("insert_batch_id"),
            F.lit(None).cast(LongType()).alias("update_batch_id"),
            F.current_timestamp().alias("insert_timestamp"),
            F.lit(None).cast(TimestampType()).alias("update_timestamp"),
        )
        records_inserted = new_rows_df.count()

        # â”€â”€ 4b. CHANGED records â€” hash mismatch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        changed_df = joined.filter(
            F.col("ods.customer_id").isNotNull()
            & (F.col("stg.row_hash") != F.col("ods.row_hash"))
        )
        records_updated = (
            changed_df.select(F.col("stg.customer_id")).distinct().count()
        )

        # Expire old current ODS versions via Delta MERGE
        expire_sks_df = (
            changed_df
            .select(F.col("ods.ods_customer_sk").alias("ods_customer_sk"))
            .distinct()
        )
        (
            DeltaTable.forName(spark, "ODS_CUSTOMER").alias("t")
            .merge(expire_sks_df.alias("s"), "t.ods_customer_sk = s.ods_customer_sk")
            .whenMatchedUpdate(set={
                "is_current":         F.lit(False),
                "effective_end_date": F.current_timestamp(),
                "update_batch_id":    F.lit(batch_id).cast(LongType()),
                "update_timestamp":   F.current_timestamp(),
            })
            .execute()
        )

        # New version rows for changed customers
        changed_new_rows_df = changed_df.select(
            *_ods_payload("stg"),
            F.current_timestamp().alias("effective_start_date"),
            F.lit(None).cast(TimestampType()).alias("effective_end_date"),
            F.lit(True).alias("is_current"),
            F.lit(batch_id).cast(LongType()).alias("insert_batch_id"),
            F.lit(None).cast(LongType()).alias("update_batch_id"),
            F.current_timestamp().alias("insert_timestamp"),
            F.lit(None).cast(TimestampType()).alias("update_timestamp"),
        )

        # â”€â”€ 4c. UNCHANGED records â€” count only, no ODS write â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        records_unchanged = joined.filter(
            F.col("ods.customer_id").isNotNull()
            & (F.col("stg.row_hash") == F.col("ods.row_hash"))
        ).count()

        # â”€â”€ 5. Write all new ODS rows (new keys + SCD new versions) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        (
            new_rows_df
            .unionByName(changed_new_rows_df)
            .withColumn("ods_customer_sk", F.monotonically_increasing_id())
            .write.format("delta").mode("append").saveAsTable("ODS_CUSTOMER")
        )

        # â”€â”€ 6. Mark staging rows as processed â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        processed_sks = (
            valid_df.select("stg_customer_sk")
            .union(reject_stg_df.select("stg_customer_sk"))
        )
        (
            DeltaTable.forName(spark, "STG_CUSTOMER").alias("t")
            .merge(processed_sks.alias("s"), "t.stg_customer_sk = s.stg_customer_sk")
            .whenMatchedUpdate(set={"is_processed": F.lit(True)})
            .execute()
        )

        stg_all.unpersist()
        ods_current.unpersist()

        batch_status = "PARTIAL" if records_rejected > 0 else "SUCCESS"

    except Exception as exc:
        batch_status  = "FAILED"
        error_message = (
            str(exc) + "\n" + "".join(tb.format_tb(exc.__traceback__))
        )[:4000]
        if stg_all is not None:
            try:
                stg_all.unpersist()
            except Exception:
                pass
        if ods_current is not None:
            try:
                ods_current.unpersist()
            except Exception:
                pass

    finally:
        # â”€â”€ 7. Close ETL batch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        if batch_id is not None:
            upd_df = spark.createDataFrame(
                [(
                    batch_id, batch_status,
                    records_read, records_inserted, records_updated,
                    records_unchanged, records_rejected, error_message,
                )],
                schema=(
                    "batch_id LONG, batch_status STRING, "
                    "records_read LONG, records_inserted LONG, records_updated LONG, "
                    "records_unchanged LONG, records_rejected LONG, "
                    "error_message STRING"
                ),
            )
            (
                DeltaTable.forName(spark, "ETLBatchControl").alias("t")
                .merge(upd_df.alias("s"), "t.batch_id = s.batch_id")
                .whenMatchedUpdate(set={
                    "batch_end_time":    F.current_timestamp(),
                    "batch_status":      F.col("s.batch_status"),
                    "records_read":      F.col("s.records_read"),
                    "records_inserted":  F.col("s.records_inserted"),
                    "records_updated":   F.col("s.records_updated"),
                    "records_unchanged": F.col("s.records_unchanged"),
                    "records_rejected":  F.col("s.records_rejected"),
                    "error_message":     F.col("s.error_message"),
                    "updated_timestamp": F.current_timestamp(),
                })
                .execute()
            )

        spark.stop()


if __name__ == "__main__":
    main()
