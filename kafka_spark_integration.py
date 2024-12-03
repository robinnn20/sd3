from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, explode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

# Configuración del SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.master", "local") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.authenticate", "false") \
    .config("spark.hadoop.security.authentication", "none") \
    .config("spark.hadoop.security.authorization", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()

# Definir el esquema para los datos JSON
schema = StructType([
    StructField("alerts", ArrayType(StructType([
        StructField("location", StructType([
            StructField("x", FloatType(), True), # Longitud
            StructField("y", FloatType(), True), # Latitud
        ])),
        StructField("severity", StringType(), True),
        StructField("type", StringType(), True),
        StructField("city", StringType(), True),
    ])), True)
])

# Configuración para leer de Kafka
kafka_streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "waze_traffic") \
    .option("startingOffsets", "earliest") \
    .load()

# Los datos de Kafka son binarios, así que los convertimos en texto
kafka_sstreaming_df = kafka_streaming_df.selectExpr("CAST(value AS STRING)")

# Convertir el JSON en un DataFrame estructurado
json_df = kafka_sstreaming_df.select(from_json(col("value"), schema).alias("data"))

# Mostrar los datos sin procesar para verificar si llegan correctamente
raw_json_df = kafka_streaming_df.select("value")
raw_json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Verificar el contenido de los datos después de procesar el JSON
processed_df = json_df.select("data.alerts")

# Usar 'explode' para convertir los arrays en filas separadas
exploded_df = processed_df.select(explode(col("alerts")).alias("alert"))

# Extraer solo los datos relevantes de las alertas
filtered_df = exploded_df.select(
    col("alert.location.x").alias("longitude"), # Longitud
    col("alert.location.y").alias("latitude"), # Latitud
    col("alert.severity").alias("severity"), # Severidad
    col("alert.type").alias("type"), # Tipo de incidente
    col("alert.city").alias("city"), # Descripción
    lit("current_timestamp()").alias("timestamp") # Marca temporal
)

# Mostrar los datos filtrados en la consola (streaming)
query = filtered_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar a que termine el stream
query.awaitTermination()
#raw_json_df.awaitTermination()
