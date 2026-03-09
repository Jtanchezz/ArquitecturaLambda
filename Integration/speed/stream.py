import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
<<<<<<< HEAD
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
=======
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
>>>>>>> e3a4689d2ed20a1d98ad3ac7c8d812e81ce23fc4

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

<<<<<<< HEAD
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

=======
>>>>>>> e3a4689d2ed20a1d98ad3ac7c8d812e81ce23fc4
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),   # viene como ISO con Z
    StructField("event_name", StringType(), True),
    StructField("user_id", StringType(), True),
<<<<<<< HEAD
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
=======
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("revenue", DoubleType(), True),
>>>>>>> e3a4689d2ed20a1d98ad3ac7c8d812e81ce23fc4
])

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
<<<<<<< HEAD
    .filter(
        col("event_time_ts").isNotNull()
        & col("event_id").isNotNull()
        & col("event_name").isNotNull()
        & col("user_id").isNotNull()
    )
=======
    .filter(col("event_time_ts").isNotNull())
>>>>>>> e3a4689d2ed20a1d98ad3ac7c8d812e81ce23fc4
)

df_agregado = (
    df_parsed
<<<<<<< HEAD
    .filter(col("product_id").isNotNull() & col("event_name").isNotNull())
=======
    .filter(col("product_id").isNotNull())
>>>>>>> e3a4689d2ed20a1d98ad3ac7c8d812e81ce23fc4
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
<<<<<<< HEAD
query.awaitTermination()
=======
query.awaitTermination()
>>>>>>> e3a4689d2ed20a1d98ad3ac7c8d812e81ce23fc4
