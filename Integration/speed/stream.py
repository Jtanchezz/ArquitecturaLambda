import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),   # viene como ISO con Z
    StructField("event_name", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("revenue", DoubleType(), True),
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
    .filter(col("event_time_ts").isNotNull())
)

df_agregado = (
    df_parsed
    .filter(col("product_id").isNotNull())
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