"""Common job for CustomerStaging to CustomerODS SCD Type 2 load"""

import sys
import os
from datetime import datetime

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sha2, concat_ws, coalesce,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, IntegerType,
)

# â”€â”€ Constants â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
SRC_SYS_CD      = "CRM"
PROCESS_NAME    = "CustomerStaging_to_CustomerODS"
SOURCE_TABLE    = "CustomerStaging"
TARGET_TABLE    = "CustomerODS"
BATCH_CTL_TABLE = "ETLBatchControl"

BUSINESS_KEY   = "customer_id"
BUSINESS_ATTRS = [
    "first_name", "last_name", "email", "phone",
    "address", "city", "state", "zip",
]


# â”€â”€ DB connection helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _jdbc_url():
    host = os.environ["DB_HOST"]
    port = os.environ.get("DB_PORT", "5432")
    db   = os.environ["DB_NAME"]
    return f"jdbc:postgresql://{host}:{port}/{db}"


def _jdbc_props():
    return {
        "user":     os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "driver":   "org.postgresql.Driver",
    }


def _pg_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def _execute(sql, params=None):
    """Execute a DML statement and commit."""
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def _fetch_one(sql, params=None):
    """Execute a query and return the first row."""
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


# â”€â”€ Spark helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _read(spark, table):
    return spark.read.jdbc(
        url=_jdbc_url(), table=table, properties=_jdbc_props()
    )


def _append(df, table):
    df.write.jdbc(
        url=_jdbc_url(), table=table, mode="append", properties=_jdbc_props()
    )


# â”€â”€ Row hash â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _compute_hash(df):
    """SHA2-256 over all business attributes concatenated with | separator."""
    return df.withColumn(
        "row_hash",
        sha2(
            concat_ws("|", *[coalesce(col(c), lit("")) for c in BUSINESS_ATTRS]),
            256,
        ),
    )


# â”€â”€ ETLBatchControl â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _start_batch(batch_ts):
    """Insert a RUNNING control row; return the generated batch_id."""
    sql = f"""
        INSERT INTO {BATCH_CTL_TABLE}
            (process_name, source_table, target_table,
             records_read, records_inserted, records_updated,
             status, start_time, end_time, load_dt)
        VALUES (%s, %s, %s, 0, 0, 0, 'RUNNING', %s, NULL, %s)
        RETURNING batch_id
    """
    row = _fetch_one(sql, (PROCESS_NAME, SOURCE_TABLE, TARGET_TABLE, batch_ts, batch_ts))
    return row[0]


def _close_batch(batch_id, records_read, records_inserted, records_updated,
                 status, end_ts):
    sql = f"""
        UPDATE {BATCH_CTL_TABLE}
           SET records_read     = %s,
               records_inserted = %s,
               records_updated  = %s,
               status           = %s,
               end_time         = %s
         WHERE batch_id = %s
    """
    _execute(sql, (records_read, records_inserted, records_updated,
                   status, end_ts, batch_id))


# â”€â”€ SCD Type 2 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _expire_rows(ods_ids, batch_ts):
    """Close out the previous active versions for changed customer_ids."""
    if not ods_ids:
        return
    placeholders = ",".join(["%s"] * len(ods_ids))
    sql = f"""
        UPDATE {TARGET_TABLE}
           SET is_current         = 'N',
               effective_end_date = %s,
               rec_stat_cd        = 'U',
               upd_dt             = %s
         WHERE customer_ods_id IN ({placeholders})
           AND is_current = 'Y'
    """
    _execute(sql, (batch_ts, batch_ts, *ods_ids))


def run_scd2(spark, batch_ts):
    """
    Execute the full SCD Type 2 merge:
      - New customer_id  â†’ INSERT  rec_stat_cd='A'
      - Changed row_hash â†’ expire old row, INSERT new version  rec_stat_cd='U'
      - Unchanged        â†’ no action
    Returns (records_read, records_inserted, records_updated).
    """
    ts_lit  = lit(batch_ts).cast(TimestampType())
    null_ts = lit(None).cast(TimestampType())

    # Read source
    stg = _compute_hash(_read(spark, SOURCE_TABLE))
    stg.cache()
    records_read = stg.count()
    print(f"[INFO] Records read from {SOURCE_TABLE}: {records_read}")

    # Read current active ODS rows for change detection
    ods_current = (
        _read(spark, TARGET_TABLE)
        .filter(col("is_current") == "Y")
        .select("customer_id", "row_hash", "customer_ods_id")
    )
    ods_current.cache()

    # Left-join staging to ODS to classify each source row
    joined = stg.alias("s").join(ods_current.alias("o"), on=BUSINESS_KEY, how="left")

    new_df     = joined.filter(col("o.customer_id").isNull())
    changed_df = joined.filter(
        col("o.customer_id").isNotNull()
        & (col("s.row_hash") != col("o.row_hash"))
    )

    # Expire stale ODS versions
    changed_ids = [
        r.customer_ods_id
        for r in changed_df
        .select(col("o.customer_ods_id").alias("customer_ods_id"))
        .collect()
    ]
    records_updated = len(changed_ids)
    _expire_rows(changed_ids, batch_ts)
    print(f"[INFO] Expired {records_updated} ODS row(s).")

    # Build the INSERT payload (new customers + new SCD2 versions)
    def _to_insert(src_df, rec_stat):
        return (
            src_df
            .select(
                col("s.customer_id"),
                col("s.first_name"),
                col("s.last_name"),
                col("s.email"),
                col("s.phone"),
                col("s.address"),
                col("s.city"),
                col("s.state"),
                col("s.zip"),
                col("s.updated_at"),
                col("s.row_hash"),
            )
            .withColumn("effective_start_date", ts_lit)
            .withColumn("effective_end_date",   null_ts)
            .withColumn("is_current",  lit("Y"))
            .withColumn("src_sys_cd",  lit(SRC_SYS_CD))
            .withColumn("rec_stat_cd", lit(rec_stat))
            .withColumn("load_dt",     ts_lit)
            .withColumn(
                "upd_dt",
                coalesce(col("s.updated_at"), ts_lit),
            )
        )

    insert_df = _to_insert(new_df, "A").unionByName(_to_insert(changed_df, "U"))
    insert_df.cache()
    records_inserted = insert_df.count()

    if records_inserted > 0:
        _append(insert_df, TARGET_TABLE)
    print(f"[INFO] Inserted {records_inserted} row(s) into {TARGET_TABLE}.")

    return records_read, records_inserted, records_updated


# â”€â”€ Entry point â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def main():
    spark    = SparkSession.builder.appName(PROCESS_NAME).getOrCreate()
    batch_ts = datetime.utcnow()

    batch_id = None
    try:
        batch_id = _start_batch(batch_ts)
        print(f"[INFO] Batch {batch_id} started at {batch_ts.isoformat()}")

        records_read, records_inserted, records_updated = run_scd2(spark, batch_ts)

        end_ts = datetime.utcnow()
        _close_batch(batch_id, records_read, records_inserted, records_updated,
                     "SUCCESS", end_ts)
        print("[INFO] Pipeline completed successfully.")

    except Exception as exc:
        print(f"[ERROR] Pipeline failed: {exc}", file=sys.stderr)
        if batch_id is not None:
            try:
                _close_batch(batch_id, 0, 0, 0, "FAILED", datetime.utcnow())
            except Exception:
                pass
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
