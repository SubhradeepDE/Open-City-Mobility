from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    when,
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
QUARANTINE_PATH = "data/quarantine/vehicle_positions"

SILVER_CHECKPOINT = (
    "data/checkpoints/pyspark_vehicle_quality_silver"
)

QUARANTINE_CHECKPOINT = (
    "data/checkpoints/pyspark_vehicle_quality_quarantine"
)


# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityDataQuality")
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
# Kafka
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
# Keep Kafka metadata + raw JSON
# --------------------------------------------------

raw_df = kafka_df.select(
    col("value")
    .cast("string")
    .alias("raw_event"),

    col("partition")
    .alias("kafka_partition"),

    col("offset")
    .alias("kafka_offset"),

    col("timestamp")
    .alias("kafka_timestamp"),
)


# --------------------------------------------------
# Parse JSON
# --------------------------------------------------

parsed_df = raw_df.select(
    col("raw_event"),

    from_json(
        col("raw_event"),
        vehicle_schema,
    ).alias("event"),

    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
)


# --------------------------------------------------
# Flatten
# --------------------------------------------------

df = parsed_df.select(
    col("raw_event"),

    col("event.event_version")
    .alias("event_version"),

    col("event.event_type")
    .alias("event_type"),

    col("event.vehicle_id")
    .alias("vehicle_id"),

    col("event.route_id")
    .alias("route_id"),

    col("event.trip_id")
    .alias("trip_id"),

    col("event.latitude")
    .alias("latitude"),

    col("event.longitude")
    .alias("longitude"),

    col("event.vehicle_timestamp")
    .alias("vehicle_timestamp"),

    col("event.ingestion_timestamp")
    .alias("ingestion_timestamp"),

    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
)


# --------------------------------------------------
# Validation flags
# --------------------------------------------------

validated_df = df.withColumn(
    "validation_error",
    when(
        col("event_version").isNull(),
        lit("missing_or_invalid_event_version"),
    )
    .when(
        col("event_version") != 1,
        lit("unsupported_event_version"),
    )
    .when(
        col("event_type").isNull(),
        lit("missing_event_type"),
    )
    .when(
        col("event_type") != "vehicle_position",
        lit("invalid_event_type"),
    )
    .when(
        col("vehicle_id").isNull(),
        lit("missing_vehicle_id"),
    )
    .when(
        col("latitude").isNull(),
        lit("missing_latitude"),
    )
    .when(
        ~col("latitude").between(-90, 90),
        lit("invalid_latitude"),
    )
    .when(
        col("longitude").isNull(),
        lit("missing_longitude"),
    )
    .when(
        ~col("longitude").between(-180, 180),
        lit("invalid_longitude"),
    )
    .when(
        col("vehicle_timestamp").isNull(),
        lit("missing_vehicle_timestamp"),
    )
    .when(
        col("ingestion_timestamp").isNull(),
        lit("missing_ingestion_timestamp"),
    )
)


# --------------------------------------------------
# Separate valid and invalid records
# --------------------------------------------------

valid_df = (
    validated_df
    .filter(
        col("validation_error").isNull()
    )
    .drop("validation_error", "raw_event")
)


invalid_df = (
    validated_df
    .filter(
        col("validation_error").isNotNull()
    )
)


# --------------------------------------------------
# Write valid events to Silver
# --------------------------------------------------

silver_query = (
    valid_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        SILVER_PATH,
    )
    .option(
        "checkpointLocation",
        SILVER_CHECKPOINT,
    )
    .partitionBy("route_id")
    .trigger(
        processingTime="30 seconds"
    )
    .start()
)


# --------------------------------------------------
# Write invalid events to Quarantine
# --------------------------------------------------

quarantine_query = (
    invalid_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        QUARANTINE_PATH,
    )
    .option(
        "checkpointLocation",
        QUARANTINE_CHECKPOINT,
    )
    .trigger(
        processingTime="30 seconds"
    )
    .start()
)


print("========================================")
print("Open City Mobility")
print("Streaming Data Quality")
print("========================================")

print(
    f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
)

print(
    f"Topic: {KAFKA_TOPIC}"
)

print(
    f"Silver: {SILVER_PATH}"
)

print(
    f"Quarantine: {QUARANTINE_PATH}"
)

print("\nStreaming...\n")


spark.streams.awaitAnyTermination()