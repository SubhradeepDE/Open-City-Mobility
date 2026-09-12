from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"


# --------------------------------------------------
# Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityKafkaStreaming")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Read from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option(
        "subscribe",
        KAFKA_TOPIC,
    )
    .option(
        "startingOffsets",
        "earliest",
    )
    .load()
)


# --------------------------------------------------
# Convert Kafka value from binary → string
# --------------------------------------------------

events_df = kafka_df.select(
    col("key")
    .cast("string")
    .alias("vehicle_key"),

    col("value")
    .cast("string")
    .alias("vehicle_event"),

    col("partition"),

    col("offset"),

    col("timestamp")
    .alias("kafka_timestamp"),
)


# --------------------------------------------------
# Write streaming output to console
# --------------------------------------------------

query = (
    events_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 5)
    .option(
        "checkpointLocation",
        "data/checkpoints/pyspark_kafka_test",
    )
    .start()
)


print("========================================")
print("PySpark Kafka Streaming")
print("========================================")
print(
    f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
)
print(
    f"Topic: {KAFKA_TOPIC}"
)
print("Waiting for Kafka events...\n")


query.awaitTermination()