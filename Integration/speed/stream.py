import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
    ArrayType,
)

TOPIC = os.getenv("TOPIC", "ecommerce-events")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

spark = (
    SparkSession.builder
    .appName("ECommerce_SpeedLayer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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
        StructField("event_time", StringType(), True),   # viene como ISO con Z
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

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    # ISO con Z -> patrón X
    .withColumn("event_time_ts", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"))
    .filter(
        col("event_time_ts").isNotNull()
        & col("event_id").isNotNull()
        & col("event_name").isNotNull()
        & col("user_id").isNotNull()
    )
)

df_agregado = (
    df_parsed
    .filter(col("product_id").isNotNull() & col("event_name").isNotNull())
    .groupBy("product_id", "event_name")
    .count()
)


def escribir_a_redis(df_batch, batch_id):
    import redis  # se instala dentro del contenedor
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # update mode => count actual => HSET (no hincrby)
    for fila in df_batch.toLocalIterator():
        redis_key = f"realtime:stats:{fila['product_id']}"
        r.hset(redis_key, fila["event_name"], int(fila["count"]))


query = (
    df_agregado.writeStream
    .outputMode("update")
    .option("checkpointLocation", "/tmp/spark_checkpoints/speedlayer")
    .foreachBatch(escribir_a_redis)
    .start()
)

print(f"⚡ Speed Layer: Kafka={KAFKA_BOOTSTRAP} topic={TOPIC} -> Redis={REDIS_HOST}:{REDIS_PORT}")
query.awaitTermination()
