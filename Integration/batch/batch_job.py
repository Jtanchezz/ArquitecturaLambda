import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
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
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("line_total", DoubleType(), True),
        ]
    ),
    True,
)

schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("cart_id", StringType(), True),
        StructField("checkout_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("cart_items_qty", IntegerType(), True),
        StructField("cart_value", DoubleType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("items", items_schema, True),
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
