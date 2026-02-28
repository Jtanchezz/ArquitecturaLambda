from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import redis

# 1. Iniciar Spark con el conector de Kafka
spark = SparkSession.builder \
    .appName("ECommerce_SpeedLayer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Mapear TU esquema JSON exacto
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("event_name", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("revenue", DoubleType(), True)
    # Nota: Omití algunos campos como 'session_id' o 'items' para mantener 
    # el procesamiento en memoria ligero, ya que en la Speed Layer solo 
    # queremos agregaciones rápidas. El resto se va a la capa Batch.
])

# 3. Leer desde Kafka (que ahora corre en tu Docker)
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsear el JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Filtrar eventos huérfanos y agrupar
# Queremos saber el total de cada tipo de evento por producto en vivo
df_agregado = df_parsed \
    .filter(col("product_id").isNotNull()) \
    .groupBy("product_id", "event_name") \
    .count()

# 6. Escribir los resultados en tu Redis de Docker
def escribir_a_redis(df_batch, batch_id):
    # Conexión al Redis local
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    for fila in df_batch.collect():
        prod_id = fila['product_id']
        evento = fila['event_name']
        cantidad = fila['count']
        
        # Creamos una llave dinámica, ej: "realtime:stats:prod_456"
        redis_key = f"realtime:stats:{prod_id}"
        
        # Incrementamos el contador específico de ese evento en el Hash
        r.hincrby(redis_key, evento, cantidad)

# 7. Ejecutar el Stream
query = df_agregado.writeStream \
    .outputMode("update") \
    .foreachBatch(escribir_a_redis) \
    .start()

print("⚡ Speed Layer escuchando a Kafka en localhost:9092 y actualizando Redis...")
query.awaitTermination()