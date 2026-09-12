import psycopg2

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

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "mobility"
POSTGRES_USER = "mobility_user"
POSTGRES_PASSWORD = "mobility_password"

CHECKPOINT_PATH = (
    "data/checkpoints/pyspark_latest_vehicle_sink"
)


# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityLatestVehicleSink")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Kafka schema
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
# Parse + validate
# --------------------------------------------------

vehicle_df = (
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
    )
    .select(
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
    )
    .filter(
        (col("event_version") == 1)
        & (
            col("event_type")
            == "vehicle_position"
        )
        & col("vehicle_id").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("vehicle_timestamp").isNotNull()
        & col("ingestion_timestamp").isNotNull()
        & col("latitude").between(-90, 90)
        & col("longitude").between(-180, 180)
    )
)


# --------------------------------------------------
# PostgreSQL upsert
# --------------------------------------------------

def upsert_batch(
    batch_df,
    batch_id,
):
    """Upsert the latest position of each vehicle."""

    if batch_df.isEmpty():
        print(
            f"Batch {batch_id}: no records"
        )
        return

    # There may be multiple events for the same
    # vehicle in a single micro-batch.
    #
    # Keep only the latest event according to
    # vehicle event time.

    latest_df = (
        batch_df
        .orderBy(
            col("vehicle_timestamp").desc()
        )
        .dropDuplicates(
            ["vehicle_id"]
        )
    )

    rows = latest_df.collect()

    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    cursor = connection.cursor()

    sql = """
        INSERT INTO latest_vehicle_positions (
            vehicle_id,
            route_id,
            trip_id,
            latitude,
            longitude,
            vehicle_timestamp,
            ingestion_timestamp,
            kafka_partition,
            kafka_offset,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (vehicle_id)
        DO UPDATE SET
            route_id = EXCLUDED.route_id,
            trip_id = EXCLUDED.trip_id,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            vehicle_timestamp =
                EXCLUDED.vehicle_timestamp,
            ingestion_timestamp =
                EXCLUDED.ingestion_timestamp,
            kafka_partition =
                EXCLUDED.kafka_partition,
            kafka_offset =
                EXCLUDED.kafka_offset,
            updated_at = NOW()
        WHERE
            latest_vehicle_positions.vehicle_timestamp
            <= EXCLUDED.vehicle_timestamp;
    """

    for row in rows:
        cursor.execute(
            sql,
            (
                row["vehicle_id"],
                row["route_id"],
                row["trip_id"],
                row["latitude"],
                row["longitude"],
                row["vehicle_timestamp"],
                row["ingestion_timestamp"],
                row["kafka_partition"],
                row["kafka_offset"],
            ),
        )

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"Batch {batch_id}: "
        f"{len(rows)} vehicle states upserted"
    )


# --------------------------------------------------
# Start streaming query
# --------------------------------------------------

query = (
    vehicle_df
    .writeStream
    .foreachBatch(upsert_batch)
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
print("Latest Vehicle State Sink")
print("========================================")

print(
    f"Kafka topic: {KAFKA_TOPIC}"
)

print(
    "PostgreSQL table: "
    "latest_vehicle_positions"
)

print("\nWaiting for events...\n")


query.awaitTermination()