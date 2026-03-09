import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
    ArrayType,
)

RAW_EVENTS_PATH = os.getenv("RAW_EVENTS_PATH", "hdfs://namenode:8020/data/raw/events")
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "default")
HIVE_EVENTS_TABLE = os.getenv("HIVE_EVENTS_TABLE", "ecommerce_events")
SPARK_WAREHOUSE = os.getenv(
    "SPARK_WAREHOUSE",
    "hdfs://namenode:8020/warehouse/tablespace/managed/hive",
)
BATCH_WRITE_MODE = os.getenv("BATCH_WRITE_MODE", "append")

items_schema = ArrayType(
    StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("line_total", DoubleType(), True),
        ]
    ),
    True,
)

customer_schema = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("is_member", BooleanType(), True),
        StructField("signup_date", StringType(), True),
    ]
)

device_schema = StructType(
    [
        StructField("device_type", StringType(), True),
        StructField("os", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("user_agent", StringType(), True),
    ]
)

geo_schema = StructType(
    [
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("ip", StringType(), True),
    ]
)

marketing_schema = StructType(
    [
        StructField("channel", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("source", StringType(), True),
        StructField("term", StringType(), True),
        StructField("content", StringType(), True),
        StructField("referrer", StringType(), True),
    ]
)

page_schema = StructType(
    [
        StructField("url", StringType(), True),
        StructField("path", StringType(), True),
        StructField("title", StringType(), True),
    ]
)

product_schema = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
    ]
)

cart_schema = StructType(
    [
        StructField("cart_id", StringType(), True),
        StructField("items_qty", IntegerType(), True),
        StructField("value", DoubleType(), True),
        StructField("coupon_code", StringType(), True),
        StructField("discount", DoubleType(), True),
        StructField("tax", DoubleType(), True),
        StructField("shipping_cost", DoubleType(), True),
    ]
)

address_schema = StructType(
    [
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("city", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("address_line1", StringType(), True),
    ]
)

order_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_provider", StringType(), True),
        StructField("installments", IntegerType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("tax", DoubleType(), True),
        StructField("shipping_cost", DoubleType(), True),
        StructField("total", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("shipping_address", address_schema, True),
        StructField("billing_address", address_schema, True),
    ]
)

schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("event_version", IntegerType(), True),
        StructField("event_source", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("customer", customer_schema, True),
        StructField("device", device_schema, True),
        StructField("geo", geo_schema, True),
        StructField("marketing", marketing_schema, True),
        StructField("page", page_schema, True),
        StructField("product_id", StringType(), True),
        StructField("product", product_schema, True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("cart", cart_schema, True),
        StructField("checkout_id", StringType(), True),
        StructField("order", order_schema, True),
        StructField("items", items_schema, True),
        StructField("revenue", DoubleType(), True),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ECommerce_BatchLayer")
        .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE)
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = (
        spark.read.schema(schema)
        .option("recursiveFileLookup", "true")
        .json(RAW_EVENTS_PATH)
    )

    df = (
        df_raw.withColumnRenamed("event_time", "event_time_raw")
        .withColumn(
            "event_time",
            to_timestamp(col("event_time_raw"), "yyyy-MM-dd'T'HH:mm:ssX"),
        )
        .withColumn("event_date", to_date(col("event_time")))
    )

    df_valid = df.filter(
        col("event_time").isNotNull()
        & col("event_id").isNotNull()
        & col("event_name").isNotNull()
        & col("user_id").isNotNull()
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    table_full = f"{HIVE_DATABASE}.{HIVE_EVENTS_TABLE}"

    table_exists = spark.catalog.tableExists(table_full)
    write_mode = BATCH_WRITE_MODE
    if write_mode == "append" and not table_exists:
        write_mode = "overwrite"

    df_valid.cache()
    total = df_valid.count()
    (
        df_valid.write.mode(write_mode)
        .format("parquet")
        .partitionBy("event_date")
        .saveAsTable(table_full)
    )
    df_valid.unpersist()
    print(
        "Batch OK: {} filas -> {} (mode={})".format(
            total, table_full, write_mode
        )
    )


if __name__ == "__main__":
    main()
