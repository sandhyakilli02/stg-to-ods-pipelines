"""
transform.py â€” Customer Order Management Pipeline
Reads raw sources (OMS, CRM, PIM, REF), applies STTM transformations,
validates referential integrity, and writes to curated Delta tables.
"""

import logging
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s â€” %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("customer_order_pipeline")

# ---------------------------------------------------------------------------
# Paths (override via config.py or environment variables as needed)
# ---------------------------------------------------------------------------
RAW_ORDERS_PATH = "/mnt/raw/orders"
RAW_CUSTOMERS_PATH = "/mnt/raw/customers"
RAW_PRODUCTS_PATH = "/mnt/raw/products"
RAW_REF_STATUS_PATH = "/mnt/raw/ref_order_status"

CURATED_ORDERS_PATH = "/mnt/curated/customer_orders"
CURATED_CUSTOMERS_PATH = "/mnt/curated/customers"
CURATED_PRODUCTS_PATH = "/mnt/curated/products"
CURATED_STATUS_PATH = "/mnt/curated/order_status"

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
VALID_ACTIVE_FLAGS = ("Y", "1", "TRUE")

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CustomerOrderManagementPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_valid_uuid_col(col_expr):
    """Return a boolean Column â€” True when the string matches UUID v4 pattern."""
    uuid_regex = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    return col_expr.rlike(uuid_regex)


def _active_flag_to_bool(col_expr):
    return F.when(F.upper(col_expr).isin(*VALID_ACTIVE_FLAGS), F.lit(True)).otherwise(
        F.lit(False)
    )


def _to_ts(col_expr):
    return F.to_timestamp(col_expr, TS_FORMAT)


def _count_or_zero(df, label: str) -> int:
    try:
        n = df.count()
        log.info("  %-40s  %d rows", label, n)
        return n
    except Exception as exc:  # noqa: BLE001
        log.warning("  Could not count '%s': %s", label, exc)
        return -1


# ---------------------------------------------------------------------------
# 1. Ingest
# ---------------------------------------------------------------------------

def ingest(spark: SparkSession):
    log.info("=== INGEST ===")
    raw_orders = spark.read.format("delta").load(RAW_ORDERS_PATH)
    raw_customers = spark.read.format("delta").load(RAW_CUSTOMERS_PATH)
    raw_products = spark.read.format("delta").load(RAW_PRODUCTS_PATH)
    ref_status = spark.read.format("delta").load(RAW_REF_STATUS_PATH)

    _count_or_zero(raw_orders, "raw_orders")
    _count_or_zero(raw_customers, "raw_customers")
    _count_or_zero(raw_products, "raw_products")
    _count_or_zero(ref_status, "ref_order_status")

    return raw_orders, raw_customers, raw_products, ref_status


# ---------------------------------------------------------------------------
# 2. Transform â€” OrderStatus (reference data, no FK deps)
# ---------------------------------------------------------------------------

def transform_order_status(ref_status_df):
    log.info("=== TRANSFORM â€” OrderStatus ===")

    status_df = ref_status_df.select(
        F.upper(F.trim(F.col("status_cd"))).cast(StringType()).alias("status_code"),
        F.trim(F.col("status_lbl")).cast(StringType()).alias("status_label"),
        F.trim(F.col("status_desc")).cast(StringType()).alias("description"),
        F.when(F.upper(F.col("terminal_flag")) == F.lit("Y"), F.lit(True))
        .otherwise(F.lit(False))
        .cast(BooleanType())
        .alias("is_terminal"),
        F.col("disp_ord").cast(IntegerType()).alias("display_order"),
    )

    # Drop rows with NULL PK
    before = _count_or_zero(status_df, "status before null-PK drop")
    status_df = status_df.dropna(subset=["status_code", "status_label"])
    after = _count_or_zero(status_df, "status after null-PK drop")
    log.info("  OrderStatus rejected (null PK/label): %d", before - after)

    return status_df


# ---------------------------------------------------------------------------
# 3. Transform â€” Customer
# ---------------------------------------------------------------------------

def transform_customers(raw_customers_df):
    log.info("=== TRANSFORM â€” Customer ===")

    # RFC-5322 simplified pattern for email validation
    email_regex = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    # Phone: keep digits, +, -, (, )
    phone_clean = F.regexp_replace(F.trim(F.col("phone_no")), r"[^\d+\-()]", "")

    customers_df = raw_customers_df.select(
        F.col("cust_id").cast(StringType()).alias("customer_id"),
        F.trim(F.col("fname")).cast(StringType()).alias("first_name"),
        F.trim(F.col("lname")).cast(StringType()).alias("last_name"),
        F.lower(F.trim(F.col("email_addr"))).cast(StringType()).alias("email"),
        phone_clean.cast(StringType()).alias("phone"),
        F.trim(F.col("addr1")).cast(StringType()).alias("address_line1"),
        F.trim(F.col("addr2")).cast(StringType()).alias("address_line2"),
        F.trim(F.col("city")).cast(StringType()).alias("city"),
        F.upper(F.trim(F.col("state_code"))).cast(StringType()).alias("state"),
        F.trim(F.col("zip")).cast(StringType()).alias("postal_code"),
        F.upper(F.trim(F.col("country_code"))).cast(StringType()).alias("country"),
        _to_ts(F.col("created_ts")).cast(TimestampType()).alias("created_at"),
        _to_ts(F.col("updated_ts")).cast(TimestampType()).alias("updated_at"),
        _active_flag_to_bool(F.col("active_flag"))
        .cast(BooleanType())
        .alias("is_active"),
    )

    # Validate UUID format for PK
    customers_df = customers_df.filter(
        _is_valid_uuid_col(F.col("customer_id"))
    )

    # Drop rows with NULL PK or email
    before = _count_or_zero(customers_df, "customers before null drop")
    customers_df = customers_df.dropna(subset=["customer_id", "email", "first_name", "last_name"])
    after_null = _count_or_zero(customers_df, "customers after null drop")
    log.info("  Customer rejected (null required fields): %d", before - after_null)

    # Validate email format
    customers_df = customers_df.filter(F.col("email").rlike(email_regex))
    after_email = _count_or_zero(customers_df, "customers after email validation")
    log.info("  Customer rejected (invalid email): %d", after_null - after_email)

    # Deduplicate on customer_id â€” keep latest updated_at
    customers_df = (
        customers_df.withColumn(
            "_rank",
            F.row_number().over(
                __window_by("customer_id", "updated_at")
            ),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    return customers_df


# ---------------------------------------------------------------------------
# 4. Transform â€” Product
# ---------------------------------------------------------------------------

def transform_products(raw_products_df):
    log.info("=== TRANSFORM â€” Product ===")

    products_df = raw_products_df.select(
        F.col("prod_id").cast(StringType()).alias("product_id"),
        F.trim(F.col("prod_name")).cast(StringType()).alias("product_name"),
        F.upper(F.trim(F.col("sku_code"))).cast(StringType()).alias("sku"),
        F.trim(F.col("category_nm")).cast(StringType()).alias("category"),
        F.col("list_price").cast(DecimalType(18, 4)).alias("unit_price"),
        F.upper(F.trim(F.col("currency_cd"))).cast(StringType()).alias("currency"),
        F.col("stock_qty").cast(IntegerType()).alias("stock_quantity"),
        F.trim(F.col("prod_desc")).cast(StringType()).alias("description"),
        _to_ts(F.col("created_ts")).cast(TimestampType()).alias("created_at"),
        _to_ts(F.col("updated_ts")).cast(TimestampType()).alias("updated_at"),
        _active_flag_to_bool(F.col("active_flag"))
        .cast(BooleanType())
        .alias("is_active"),
    )

    # Validate UUID format for PK
    products_df = products_df.filter(
        _is_valid_uuid_col(F.col("product_id"))
    )

    # Drop rows with NULL PK or unit_price
    before = _count_or_zero(products_df, "products before null drop")
    products_df = products_df.dropna(
        subset=["product_id", "product_name", "sku", "unit_price", "currency"]
    )
    after_null = _count_or_zero(products_df, "products after null drop")
    log.info("  Product rejected (null required fields): %d", before - after_null)

    # Reject negative price
    products_df = products_df.filter(F.col("unit_price") >= 0)
    after_price = _count_or_zero(products_df, "products after price filter")
    log.info("  Product rejected (negative price): %d", after_null - after_price)

    # Reject negative stock
    products_df = products_df.filter(F.col("stock_quantity") >= 0)

    # Deduplicate on product_id â€” keep latest updated_at
    products_df = (
        products_df.withColumn(
            "_rank",
            F.row_number().over(__window_by("product_id", "updated_at")),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    return products_df


# ---------------------------------------------------------------------------
# 5. Transform â€” CustomerOrder
# ---------------------------------------------------------------------------

def transform_orders(raw_orders_df, customers_df, products_df, status_df):
    log.info("=== TRANSFORM â€” CustomerOrder ===")

    orders_df = raw_orders_df.select(
        F.col("order_id").cast(StringType()).alias("order_id"),
        F.col("customer_id").cast(StringType()).alias("customer_id"),
        F.col("product_id").cast(StringType()).alias("product_id"),
        F.col("qty").cast(IntegerType()).alias("quantity"),
        _to_ts(F.col("order_dt")).cast(TimestampType()).alias("order_date"),
        F.upper(F.trim(F.col("order_status"))).cast(StringType()).alias("status"),
        F.col("price").cast(DecimalType(18, 4)).alias("unit_price"),
        (
            F.col("price").cast(DecimalType(18, 4))
            * F.col("qty").cast(IntegerType())
        )
        .cast(DecimalType(18, 4))
        .alias("total_amount"),
        F.coalesce(
            _to_ts(F.col("created_ts")), F.current_timestamp()
        )
        .cast(TimestampType())
        .alias("created_at"),
        F.coalesce(
            _to_ts(F.col("updated_ts")), F.current_timestamp()
        )
        .cast(TimestampType())
        .alias("updated_at"),
        F.lit(False).cast(BooleanType()).alias("is_deleted"),
    )

    # --- Validate UUID format for key columns ---
    orders_df = orders_df.filter(
        _is_valid_uuid_col(F.col("order_id"))
        & _is_valid_uuid_col(F.col("customer_id"))
        & _is_valid_uuid_col(F.col("product_id"))
    )

    # --- Drop NULL mandatory keys ---
    before = _count_or_zero(orders_df, "orders before null-key drop")
    orders_df = orders_df.dropna(
        subset=["order_id", "customer_id", "product_id", "order_date", "unit_price"]
    )
    after_null = _count_or_zero(orders_df, "orders after null-key drop")
    log.info("  Orders rejected (null mandatory fields): %d", before - after_null)

    # --- Reject quantity <= 0 ---
    orders_df = orders_df.filter(F.col("quantity") > 0)
    after_qty = _count_or_zero(orders_df, "orders after quantity filter")
    log.info("  Orders rejected (qty <= 0): %d", after_null - after_qty)

    # --- Reject negative unit_price ---
    orders_df = orders_df.filter(F.col("unit_price") >= 0)
    after_price = _count_or_zero(orders_df, "orders after price filter")
    log.info("  Orders rejected (negative price): %d", after_qty - after_price)

    # --- Default NULL status â†’ PENDING ---
    orders_df = orders_df.withColumn(
        "status",
        F.when(F.col("status").isNull() | (F.col("status") == ""), F.lit("PENDING")).otherwise(
            F.col("status")
        ),
    )

    # --- FK: customer_id must exist in customers ---
    valid_customers = customers_df.select(
        F.col("customer_id").alias("_cust_id")
    )
    orders_df = orders_df.join(
        valid_customers, orders_df["customer_id"] == valid_customers["_cust_id"], "inner"
    ).drop("_cust_id")
    after_cust = _count_or_zero(orders_df, "orders after customer FK filter")
    log.info("  Orders rejected (orphan customer_id): %d", after_price - after_cust)

    # --- FK: product_id must exist in products ---
    valid_products = products_df.select(
        F.col("product_id").alias("_prod_id")
    )
    orders_df = orders_df.join(
        valid_products, orders_df["product_id"] == valid_products["_prod_id"], "inner"
    ).drop("_prod_id")
    after_prod = _count_or_zero(orders_df, "orders after product FK filter")
    log.info("  Orders rejected (orphan product_id): %d", after_cust - after_prod)

    # --- FK: status must exist in OrderStatus ---
    valid_statuses = status_df.select(
        F.col("status_code").alias("_status_code")
    )
    orders_df = orders_df.join(
        valid_statuses, orders_df["status"] == valid_statuses["_status_code"], "inner"
    ).drop("_status_code")
    after_status = _count_or_zero(orders_df, "orders after status FK filter")
    log.info("  Orders rejected (invalid status): %d", after_prod - after_status)

    # --- Recompute total_amount after all filters (ensures consistency) ---
    orders_df = orders_df.withColumn(
        "total_amount",
        (F.col("unit_price") * F.col("quantity")).cast(DecimalType(18, 4)),
    )

    # --- Canonical column order ---
    orders_df = orders_df.select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "order_date",
        "status",
        "unit_price",
        "total_amount",
        "created_at",
        "updated_at",
        "is_deleted",
    )

    return orders_df


# ---------------------------------------------------------------------------
# 6. Write
# ---------------------------------------------------------------------------

def write_overwrite(df, path: str, label: str):
    log.info("Writing %s â†’ %s", label, path)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(path)
    )
    log.info("  Done â€” %s written.", label)


def write_merge_orders(spark: SparkSession, orders_df, path: str):
    """Delta MERGE on order_id â€” insert new, update existing."""
    log.info("Upserting CustomerOrders â†’ %s", path)

    # Ensure target exists; if not, do initial write
    try:
        target = DeltaTable.forPath(spark, path)
        (
            target.alias("t")
            .merge(orders_df.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdate(
                set={
                    "customer_id": "s.customer_id",
                    "product_id": "s.product_id",
                    "quantity": "s.quantity",
                    "order_date": "s.order_date",
                    "status": "s.status",
                    "unit_price": "s.unit_price",
                    "total_amount": "s.total_amount",
                    # Keep original created_at; refresh updated_at
                    "updated_at": F.current_timestamp(),
                    "is_deleted": "s.is_deleted",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("  Delta MERGE complete.")
    except Exception as exc:  # noqa: BLE001
        if "is not a Delta table" in str(exc) or "doesn't exist" in str(exc).lower():
            log.info("  Target does not exist â€” performing initial write.")
            (
                orders_df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(path)
            )
        else:
            raise


# ---------------------------------------------------------------------------
# 7. Audit
# ---------------------------------------------------------------------------

def audit(raw_orders_df, final_orders_df):
    log.info("=== AUDIT SUMMARY ===")
    raw_cnt = _count_or_zero(raw_orders_df, "raw_orders (source total)")
    written_cnt = _count_or_zero(final_orders_df, "customer_orders (written)")
    rejected = raw_cnt - written_cnt if raw_cnt >= 0 and written_cnt >= 0 else "unknown"
    log.info("  Raw orders       : %s", raw_cnt)
    log.info("  Written orders   : %s", written_cnt)
    log.info("  Rejected records : %s", rejected)
    log.info(
        "  Pipeline run UTC : %s",
        datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    )


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def __window_by(partition_col: str, order_col: str):
    from pyspark.sql.window import Window

    return Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession initialised. App: %s", spark.sparkContext.appName)

    try:
        # 1. Ingest
        raw_orders, raw_customers, raw_products, ref_status = ingest(spark)

        # 2. Transform reference / dimension tables (no cross-table deps)
        status_df = transform_order_status(ref_status)
        customers_df = transform_customers(raw_customers)
        products_df = transform_products(raw_products)

        # 3. Transform fact table (depends on dim tables for FK validation)
        orders_df = transform_orders(raw_orders, customers_df, products_df, status_df)

        # 4. Cache final orders for reuse in audit
        orders_df.cache()

        # 5. Write
        write_overwrite(status_df, CURATED_STATUS_PATH, "OrderStatus")
        write_overwrite(customers_df, CURATED_CUSTOMERS_PATH, "Customer")
        write_overwrite(products_df, CURATED_PRODUCTS_PATH, "Product")
        write_merge_orders(spark, orders_df, CURATED_ORDERS_PATH)

        # 6. Audit
        audit(raw_orders, orders_df)

        log.info("Pipeline completed successfully.")
    except Exception:
        log.exception("Pipeline FAILED â€” see stack trace above.")
        raise
    finally:
        spark.stop()
        log.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
