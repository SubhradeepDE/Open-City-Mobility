from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityVehicleParser")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Kafka JSON schema
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
    .load()
)


# --------------------------------------------------
# Convert Kafka binary value → string
# --------------------------------------------------

raw_df = kafka_df.select(
    col("value")
    .cast("string")
    .alias("json_value"),

    col("key")
    .cast("string")
    .alias("vehicle_key"),

    col("partition"),

    col("offset"),

    col("timestamp")
    .alias("kafka_timestamp"),
)


# --------------------------------------------------
# Parse JSON
# --------------------------------------------------

parsed_df = raw_df.select(
    from_json(
        col("json_value"),
        vehicle_schema,
    ).alias("vehicle"),

    col("vehicle_key"),
    col("partition"),
    col("offset"),
    col("kafka_timestamp"),
)


# --------------------------------------------------
# Flatten the struct
# --------------------------------------------------

vehicle_df = parsed_df.select(
    col("vehicle.event_version")
    .alias("event_version"),

    col("vehicle.event_type")
    .alias("event_type"),

    col("vehicle.vehicle_id")
    .alias("vehicle_id"),

    col("vehicle.route_id")
    .alias("route_id"),

    col("vehicle.trip_id")
    .alias("trip_id"),

    col("vehicle.latitude")
    .alias("latitude"),

    col("vehicle.longitude")
    .alias("longitude"),

    col("vehicle.vehicle_timestamp")
    .alias("vehicle_timestamp"),

    col("vehicle.ingestion_timestamp")
    .alias("ingestion_timestamp"),

    col("vehicle_key"),

    col("partition"),

    col("offset"),

    col("kafka_timestamp"),
)


# --------------------------------------------------
# Display parsed events
# --------------------------------------------------

query = (
    vehicle_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 5)
    .option(
        "checkpointLocation",
        "data/checkpoints/pyspark_vehicle_parser",
    )
    .start()
)


print("========================================")
print("PySpark Vehicle JSON Parser")
print("========================================")
print(
    f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
)
print(
    f"Topic: {KAFKA_TOPIC}"
)
print("Parsing vehicle events...\n")


query.awaitTermination()