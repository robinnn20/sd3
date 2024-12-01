from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Definición del esquema para los datos de eventos
# Esquema para los datos anidados
schema = StructType([
    StructField("timestamp", StructType([
        StructField("seconds", FloatType(), True),
        StructField("nanos", FloatType(), True)
    ])),
    StructField("insertId", StringType(), True),
    StructField("jsonPayload", StructType([
        StructField("fields", StructType([
            StructField("event_name", StructType([
                StructField("stringValue", StringType(), True)
            ])),
            StructField("location", StructType([
                StructField("stringValue", StringType(), True)
            ])),
            StructField("event_date", StructType([
                StructField("stringValue", StringType(), True)
            ])),
            StructField("device", StructType([
                StructField("structValue", StructType([
                    StructField("fields", StructType([
                        StructField("device_type", StructType([
                            StructField("stringValue", StringType(), True)
                        ])),
                        StructField("browser", StructType([
                            StructField("stringValue", StringType(), True)
                        ]))
                    ]))
                ]))
            ]))
        ]))
    ]))
])
# Configuración del SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.master", "local") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Configuración para leer de Kafka
kafka_streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "waze_traffic") \
    .option("startingOffsets", "earliest") \
    .load()

event_data = kafka_streaming_df.selectExpr("CAST(value AS STRING)")

parsed_df = event_data \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.jsonPayload.fields.event_name.stringValue").alias("event_name"),
        col("data.jsonPayload.fields.location.stringValue").alias("location"),
        col("data.jsonPayload.fields.event_date.stringValue").alias("event_date"),
        col("data.jsonPayload.fields.device.structValue.fields.device_type.stringValue").alias("device_type"),
        col("data.jsonPayload.fields.device.structValue.fields.browser.stringValue").alias("browser")
    )

# Realizar análisis (por ejemplo, contar eventos por tipo)
filtered_df = parsed_df.filter(col("event_name")== 'STILL_HERE')
# Mostrar los datos filtrados y agregados en la consola
query = filtered_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar a que termine el stream
query.awaitTermination()
