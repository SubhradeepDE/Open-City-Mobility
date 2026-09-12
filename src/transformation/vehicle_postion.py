import os
from datetime import datetime, timezone

import pandas as pd
from google.transit import gtfs_realtime_pb2


BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"


def get_latest_bronze_file() -> str:
    """Return the most recently created Bronze GTFS file."""

    files = [
        os.path.join(BRONZE_DIR, file)
        for file in os.listdir(BRONZE_DIR)
        if file.endswith(".bin")
    ]

    if not files:
        raise FileNotFoundError("No Bronze GTFS files found.")

    return max(files, key=os.path.getmtime)


def decode_vehicle_positions(file_path: str) -> list[dict]:
    """Decode a GTFS-Realtime binary file into vehicle records."""

    with open(file_path, "rb") as file:
        raw_data = file.read()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_data)

    ingestion_timestamp = datetime.now(timezone.utc)

    records = []

    for entity in feed.entity:

        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle

        vehicle_id = None
        trip_id = None
        route_id = None
        latitude = None
        longitude = None
        vehicle_timestamp = None
        vehicle_status = None

        if vehicle.HasField("vehicle"):
            vehicle_id = vehicle.vehicle.id

        if vehicle.HasField("trip"):
            trip_id = vehicle.trip.trip_id
            route_id = vehicle.trip.route_id

        if vehicle.HasField("position"):
            latitude = vehicle.position.latitude
            longitude = vehicle.position.longitude

        if vehicle.HasField("timestamp"):
            vehicle_timestamp = datetime.fromtimestamp(
                vehicle.timestamp,
                tz=timezone.utc
            )

        if vehicle.HasField("current_status"):
            vehicle_status = str(vehicle.current_status)

        records.append(
            {
                "vehicle_id": vehicle_id,
                "route_id": route_id,
                "trip_id": trip_id,
                "latitude": latitude,
                "longitude": longitude,
                "vehicle_timestamp": vehicle_timestamp,
                "ingestion_timestamp": ingestion_timestamp,
                "vehicle_status": vehicle_status,
            }
        )

    return records


def clean_vehicle_positions(records: list[dict]) -> pd.DataFrame:
    """Clean and validate vehicle position records."""

    df = pd.DataFrame(records)

    if df.empty:
        return df

    # Remove exact duplicate records
    df = df.drop_duplicates()

    # Remove records without a vehicle ID
    df = df.dropna(subset=["vehicle_id"])

    # Convert numeric columns
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # Remove impossible geographical coordinates
    df = df[
        df["latitude"].between(-90, 90)
        & df["longitude"].between(-180, 180)
    ]

    # Remove records without a valid vehicle timestamp
    df = df.dropna(subset=["vehicle_timestamp"])

    # Sort by vehicle and event time
    df = df.sort_values(
        by=["vehicle_id", "vehicle_timestamp"]
    )

    return df.reset_index(drop=True)


def save_silver(df: pd.DataFrame) -> str:
    """Save cleaned vehicle data as Parquet."""

    os.makedirs(SILVER_DIR, exist_ok=True)

    output_file = os.path.join(
        SILVER_DIR,
        "vehicle_positions.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    return output_file


def main():
    print("Starting Silver transformation...")

    bronze_file = get_latest_bronze_file()

    print(f"Reading Bronze file: {bronze_file}")

    records = decode_vehicle_positions(bronze_file)

    print(f"Decoded records: {len(records)}")

    df = clean_vehicle_positions(records)

    print(f"Clean records: {len(df)}")

    output_file = save_silver(df)

    print(f"Silver table saved: {output_file}")

    print("\nSample records:")
    print(df.head())


if __name__ == "__main__":
    main()