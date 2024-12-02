from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Configuración del SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.master", "local") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.authenticate", "false") \
    .config("spark.hadoop.security.authentication", "none") \
    .config("spark.hadoop.security.authorization", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate() # Este es el final de la configuración de SparkSession

# Configuración para leer de Kafka
kafka_streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "waze_traffic") \
    .option("startingOffsets", "earliest") \
    .load()

# Los datos de Kafka son binarios, así que los convertimos en texto
kafka_streaming_df = kafka_streaming_df.selectExpr( "CAST(value AS STRING)")

# Mostrar los datos en la consola
query = kafka_streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar a que termine el stream
query.awaitTermination()
