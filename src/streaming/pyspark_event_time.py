from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"

CHECKPOINT_PATH = (
    "data/checkpoints/pyspark_event_time"
)


# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityEventTime")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Event schema
# --------------------------------------------------

vehicle_schema = StructType([
    StructField("event_version", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("vehicle_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])


# --------------------------------------------------
# Kafka stream
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
        "latest",
    )
    .option(
        "failOnDataLoss",
        "false",
    )
    .load()
)


# --------------------------------------------------
# Parse JSON
# --------------------------------------------------

events = kafka_df.select(
    from_json(
        col("value").cast("string"),
        vehicle_schema,
    ).alias("event")
)


events = events.select(
    col("event.vehicle_id").alias("vehicle_id"),
    col("event.route_id").alias("route_id"),
    col("event.trip_id").alias("trip_id"),
    col("event.latitude").alias("latitude"),
    col("event.longitude").alias("longitude"),

    to_timestamp(
        col("event.vehicle_timestamp")
    ).alias("vehicle_timestamp"),

    to_timestamp(
        col("event.ingestion_timestamp")
    ).alias("ingestion_timestamp"),
)


# --------------------------------------------------
# Basic validation
# --------------------------------------------------

events = events.filter(
    col("vehicle_id").isNotNull()
    & col("vehicle_timestamp").isNotNull()
)


# --------------------------------------------------
# EVENT TIME
# --------------------------------------------------

# Tell Spark that vehicle_timestamp represents
# the actual time at which the event happened.

event_time_df = events.withWatermark(
    "vehicle_timestamp",
    "2 minutes",
)


# --------------------------------------------------
# 1-minute event-time window
# --------------------------------------------------

windowed = (
    event_time_df
    .groupBy(
        window(
            col("vehicle_timestamp"),
            "1 minute",
        ),
    )
    .count()
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("count").alias("vehicle_events"),
    )
)


# --------------------------------------------------
# Console output
# --------------------------------------------------

query = (
    windowed.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(
        processingTime="30 seconds"
    )
    .start()
)


print("========================================")
print("Open City Mobility")
print("Event-Time Streaming")
print("========================================")
print(
    f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
)
print(
    f"Topic: {KAFKA_TOPIC}"
)
print(
    "Event-time window: 1 minute"
)
print(
    "Watermark: 2 minutes"
)
print("\nWaiting for events...\n")


query.awaitTermination()