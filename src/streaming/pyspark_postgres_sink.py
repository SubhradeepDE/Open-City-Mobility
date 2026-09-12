from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"

POSTGRES_URL = (
    "jdbc:postgresql://localhost:5432/mobility"
)

POSTGRES_TABLE = "vehicle_positions"

POSTGRES_PROPERTIES = {
    "user": "mobility_user",
    "password": "mobility_password",
    "driver": "org.postgresql.Driver",
}

CHECKPOINT_PATH = (
    "data/checkpoints/pyspark_postgres_sink"
)


# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityPostgresSink")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Kafka event schema
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

vehicle_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            vehicle_schema,
        ).alias("event"),

        col("partition").alias(
            "kafka_partition"
        ),

        col("offset").alias(
            "kafka_offset"
        ),
    )
    .select(
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

        to_timestamp(
            col("event.vehicle_timestamp")
        ).alias("vehicle_timestamp"),

        to_timestamp(
            col("event.ingestion_timestamp")
        ).alias("ingestion_timestamp"),

        col("kafka_partition"),
        col("kafka_offset"),
    )
)


# --------------------------------------------------
# Validate
# --------------------------------------------------

valid_df = (
    vehicle_df
    .filter(col("event_version") == 1)
    .filter(
        col("event_type")
        == "vehicle_position"
    )
    .filter(
        col("vehicle_id").isNotNull()
    )
    .filter(
        col("latitude").isNotNull()
        & col("longitude").isNotNull()
    )
    .filter(
        col("latitude").between(-90, 90)
    )
    .filter(
        col("longitude").between(-180, 180)
    )
    .filter(
        col("vehicle_timestamp").isNotNull()
    )
    .filter(
        col("ingestion_timestamp").isNotNull()
    )
)


# --------------------------------------------------
# PostgreSQL writer
# --------------------------------------------------

def write_to_postgres(
    batch_df,
    batch_id,
):
    """Write one Spark micro-batch to PostgreSQL."""

    if batch_df.isEmpty():
        print(
            f"Batch {batch_id}: no records"
        )
        return

    output_df = batch_df.select(
        "vehicle_id",
        "route_id",
        "trip_id",
        "latitude",
        "longitude",
        "vehicle_timestamp",
        "ingestion_timestamp",
        "kafka_partition",
        "kafka_offset",
    )

    output_df.write \
        .jdbc(
            url=POSTGRES_URL,
            table=POSTGRES_TABLE,
            mode="append",
            properties=POSTGRES_PROPERTIES,
        )

    print(
        f"Batch {batch_id}: "
        f"{output_df.count()} records written"
    )


# --------------------------------------------------
# Start query
# --------------------------------------------------

query = (
    valid_df.writeStream
    .foreachBatch(write_to_postgres)
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
print("PySpark → PostgreSQL")
print("========================================")

print(
    f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
)

print(
    f"PostgreSQL: {POSTGRES_URL}"
)

print(
    f"Table: {POSTGRES_TABLE}"
)

print("\nWaiting for events...\n")


query.awaitTermination()