from typing import Iterator

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.streaming.stateful_processor import (
    StatefulProcessor,
    StatefulProcessorHandle,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"

OUTPUT_PATH = "data/silver/stateful_vehicle_positions"

CHECKPOINT_PATH = (
    "data/checkpoints/stateful_vehicle_tracking"
)


# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("OpenCityMobilityStatefulTracking")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# Kafka event schema
# --------------------------------------------------

vehicle_schema = StructType([
    StructField(
        "event_version",
        StringType(),
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
])


# --------------------------------------------------
# Read Kafka
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

events = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            vehicle_schema,
        ).alias("event")
    )
    .select(
        col("event.vehicle_id").alias("vehicle_id"),
        col("event.route_id").alias("route_id"),
        col("event.trip_id").alias("trip_id"),
        col("event.latitude").alias("latitude"),
        col("event.longitude").alias("longitude"),

        to_timestamp(
            col("event.vehicle_timestamp")
        ).alias("vehicle_timestamp"),
    )
    .filter(
        col("vehicle_id").isNotNull()
        & col("vehicle_timestamp").isNotNull()
    )
)


# --------------------------------------------------
# Stateful processor
# --------------------------------------------------

class LatestVehiclePosition(StatefulProcessor):
    """
    Maintain the latest known position for each vehicle.

    The grouping key is vehicle_id.
    """

    def init(
        self,
        handle: StatefulProcessorHandle,
    ) -> None:

        self.handle = handle

        state_schema = StructType([
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
        ])

        self.state = handle.getValueState(
            "vehicle_state",
            state_schema,
        )

    def handleInitialState(
        self,
        key,
        initialState,
        timerValues,
    ):
        # No initial state required for this project.
        pass

    def handleInputRows(
        self,
        key,
        rows: Iterator[pd.DataFrame],
        timerValues,
    ):

        vehicle_id = key[0]

        latest = None

        # Each pandas DataFrame contains events
        # belonging to this vehicle.
        for pdf in rows:

            if pdf.empty:
                continue

            pdf = pdf.sort_values(
                "vehicle_timestamp"
            )

            latest = pdf.iloc[-1]

        if latest is None:
            return

        latest_state = (
            latest["route_id"],
            latest["trip_id"],
            float(latest["latitude"]),
            float(latest["longitude"]),
            str(latest["vehicle_timestamp"]),
        )

        # Save the latest position as state.
        self.state.update(latest_state)

        yield pd.DataFrame(
            [
                {
                    "vehicle_id": vehicle_id,
                    "route_id": latest_state[0],
                    "trip_id": latest_state[1],
                    "latitude": latest_state[2],
                    "longitude": latest_state[3],
                    "vehicle_timestamp": latest_state[4],
                }
            ]
        )


# --------------------------------------------------
# Output schema
# --------------------------------------------------

output_schema = StructType([
    StructField(
        "vehicle_id",
        StringType(),
        False,
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
        False,
    ),
])


# --------------------------------------------------
# Apply stateful processing
# --------------------------------------------------

stateful_df = (
    events
    .groupBy("vehicle_id")
    .transformWithStateInPandas(
        statefulProcessor=LatestVehiclePosition(),
        outputStructType=output_schema,
        outputMode="Update",
        timeMode="None",
    )
)


# --------------------------------------------------
# Write results
# --------------------------------------------------

query = (
    stateful_df
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        OUTPUT_PATH,
    )
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
print("Stateful Vehicle Tracking")
print("========================================")

print(
    f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
)

print(
    f"Topic: {KAFKA_TOPIC}"
)

print(
    f"Output: {OUTPUT_PATH}"
)

print(
    "State key: vehicle_id"
)

print(
    "Maintained state: latest vehicle position"
)

print("\nWaiting for events...\n")


query.awaitTermination()