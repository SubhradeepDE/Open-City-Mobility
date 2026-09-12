import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"

SILVER_PATH = "data/silver/streaming"
CHECKPOINT_PATH = "data/checkpoints/pyspark_vehicle_silver"


# --------------------------------------------------
# Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityStreamingSilver")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Vehicle event schema
# --------------------------------------------------

vehicle_schema = StructType([
    StructField(
        "event_version",
        IntegerType(),
        True,
    ),
    StructField(
        "event_type",
        StringType(),
        True,
    ),
    StructField(
        "vehicle_id",
        StringType(),
        True,
    ),
    StructField(
        "route_id",
        StringType(),
        True,
    ),
    StructField(
        "trip_id",
        StringType(),
        True,
    ),
    StructField(
        "latitude",
        DoubleType(),
        True,
    ),
    StructField(
        "longitude",
        DoubleType(),
        True,
    ),
    StructField(
        "vehicle_timestamp",
        StringType(),
        True,
    ),
    StructField(
        "ingestion_timestamp",
        StringType(),
        True,
    ),
])


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
    .option(
        "failOnDataLoss",
        "false",
    )
    .load()
)


# --------------------------------------------------
# Parse Kafka value
# --------------------------------------------------

parsed_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            vehicle_schema,
        ).alias("event"),

        col("partition")
        .alias("kafka_partition"),

        col("offset")
        .alias("kafka_offset"),

        col("timestamp")
        .alias("kafka_timestamp"),
    )
)


# --------------------------------------------------
# Flatten event
# --------------------------------------------------

vehicle_df = parsed_df.select(
    col("event.event_version").alias(
        "event_version"
    ),

    col("event.event_type").alias(
        "event_type"
    ),

    col("event.vehicle_id").alias(
        "vehicle_id"
    ),

    col("event.route_id").alias(
        "route_id"
    ),

    col("event.trip_id").alias(
        "trip_id"
    ),

    col("event.latitude").alias(
        "latitude"
    ),

    col("event.longitude").alias(
        "longitude"
    ),

    to_timestamp(
        col("event.vehicle_timestamp")
    ).alias(
        "vehicle_timestamp"
    ),

    to_timestamp(
        col("event.ingestion_timestamp")
    ).alias(
        "ingestion_timestamp"
    ),

    col("kafka_partition"),

    col("kafka_offset"),

    col("kafka_timestamp"),
)


# --------------------------------------------------
# Data quality filters
# --------------------------------------------------

silver_df = (
    vehicle_df

    # Only our current event schema.
    .filter(
        col("event_version") == 1
    )

    # Correct event type.
    .filter(
        col("event_type") == "vehicle_position"
    )

    # Vehicle ID is mandatory.
    .filter(
        col("vehicle_id").isNotNull()
    )

    # Coordinates must exist.
    .filter(
        col("latitude").isNotNull()
        & col("longitude").isNotNull()
    )

    # Valid latitude.
    .filter(
        col("latitude").between(-90, 90)
    )

    # Valid longitude.
    .filter(
        col("longitude").between(-180, 180)
    )

    # Vehicle event timestamp must exist.
    .filter(
        col("vehicle_timestamp").isNotNull()
    )

    # Pipeline ingestion timestamp must exist.
    .filter(
        col("ingestion_timestamp").isNotNull()
    )
)


# --------------------------------------------------
# Write Streaming Silver
# --------------------------------------------------

query = (
    silver_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        SILVER_PATH,
    )
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .partitionBy(
        "route_id"
    )
    .trigger(
        processingTime="30 seconds"
    )
    .start()
)


print("========================================")
print("Open City Mobility")
print("Streaming Silver Pipeline")
print("========================================")

print(
    f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
)

print(
    f"Topic: {KAFKA_TOPIC}"
)

print(
    f"Silver path: {SILVER_PATH}"
)

print(
    "Waiting for Kafka events..."
)

query.awaitTermination()