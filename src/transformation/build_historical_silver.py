import os
from datetime import datetime, timezone

import pandas as pd
from google.transit import gtfs_realtime_pb2


BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
OUTPUT_FILE = os.path.join(
    SILVER_DIR,
    "vehicle_positions_history.parquet",
)


def get_bronze_files() -> list[str]:
    """Return all Bronze GTFS-Realtime files."""

    if not os.path.exists(BRONZE_DIR):
        raise FileNotFoundError(
            f"Bronze directory does not exist: {BRONZE_DIR}"
        )

    files = [
        os.path.join(BRONZE_DIR, file)
        for file in os.listdir(BRONZE_DIR)
        if file.endswith(".bin")
    ]

    if not files:
        raise FileNotFoundError(
            "No Bronze GTFS-Realtime files found."
        )

    return sorted(files)


def decode_file(file_path: str) -> list[dict]:
    """Decode one GTFS-Realtime file."""

    with open(file_path, "rb") as file:
        raw_data = file.read()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_data)

    # Timestamp for when our pipeline read this Bronze file.
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
                tz=timezone.utc,
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
                "source_file": os.path.basename(file_path),
            }
        )

    return records


def build_historical_dataset(
    bronze_files: list[str],
) -> pd.DataFrame:
    """Decode and combine all Bronze files."""

    all_records = []

    for index, file_path in enumerate(bronze_files, start=1):

        print(
            f"[{index}/{len(bronze_files)}] "
            f"Processing {os.path.basename(file_path)}"
        )

        records = decode_file(file_path)

        print(f"    Records found: {len(records)}")

        all_records.extend(records)

    if not all_records:
        raise ValueError(
            "No vehicle records were found in Bronze files."
        )

    return pd.DataFrame(all_records)


def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the historical vehicle dataset."""

    # Remove exact duplicates.
    df = df.drop_duplicates()

    # Require the most important fields.
    df = df.dropna(
        subset=[
            "vehicle_id",
            "latitude",
            "longitude",
            "vehicle_timestamp",
        ]
    )

    # Standardize numeric columns.
    df["latitude"] = pd.to_numeric(
        df["latitude"],
        errors="coerce",
    )

    df["longitude"] = pd.to_numeric(
        df["longitude"],
        errors="coerce",
    )

    # Remove invalid geographic coordinates.
    df = df[
        df["latitude"].between(-90, 90)
        & df["longitude"].between(-180, 180)
    ]

    # Make sure timestamps are datetime values.
    df["vehicle_timestamp"] = pd.to_datetime(
        df["vehicle_timestamp"],
        utc=True,
        errors="coerce",
    )

    df["ingestion_timestamp"] = pd.to_datetime(
        df["ingestion_timestamp"],
        utc=True,
        errors="coerce",
    )

    # Remove records whose timestamp couldn't be parsed.
    df = df.dropna(
        subset=["vehicle_timestamp"]
    )

    # Sort so each vehicle's movement is easy to follow.
    df = df.sort_values(
        by=[
            "vehicle_id",
            "vehicle_timestamp",
        ]
    )

    return df.reset_index(drop=True)


def save_dataset(df: pd.DataFrame) -> None:
    """Save historical Silver data."""

    os.makedirs(SILVER_DIR, exist_ok=True)

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"\nHistorical Silver saved to: "
        f"{OUTPUT_FILE}"
    )


def main():
    print("========================================")
    print("Building Historical Silver Dataset")
    print("========================================\n")

    bronze_files = get_bronze_files()

    print(
        f"Bronze files found: {len(bronze_files)}\n"
    )

    df = build_historical_dataset(
        bronze_files
    )

    print(
        f"\nTotal raw historical records: "
        f"{len(df)}"
    )

    df = clean_dataset(df)

    print(
        f"Total clean historical records: "
        f"{len(df)}"
    )

    print(
        f"Unique vehicles: "
        f"{df['vehicle_id'].nunique()}"
    )

    print(
        f"Unique routes: "
        f"{df['route_id'].nunique()}"
    )

    print(
        f"Time range: "
        f"{df['vehicle_timestamp'].min()} "
        f"→ "
        f"{df['vehicle_timestamp'].max()}"
    )

    save_dataset(df)

    print("\nSample data:\n")
    print(
        df.head(10).to_string(index=False)
    )


if __name__ == "__main__":
    main()