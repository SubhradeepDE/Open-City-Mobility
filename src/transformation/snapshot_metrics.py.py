import os
from datetime import datetime, timezone
import re

import pandas as pd
from google.transit import gtfs_realtime_pb2


BRONZE_DIR = "data/bronze"
GOLD_DIR = "data/gold"

OUTPUT_FILE = os.path.join(
    GOLD_DIR,
    "snapshot_metrics.parquet",
)


def get_bronze_files() -> list[str]:
    """Get all Bronze GTFS-Realtime files."""

    if not os.path.exists(BRONZE_DIR):
        raise FileNotFoundError(
            f"Bronze directory not found: {BRONZE_DIR}"
        )

    files = [
        os.path.join(BRONZE_DIR, file)
        for file in os.listdir(BRONZE_DIR)
        if file.endswith(".bin")
    ]

    if not files:
        raise FileNotFoundError(
            "No Bronze files found."
        )

    return sorted(files)

def get_ingestion_timestamp(file_path: str) -> datetime:
    """
    Extract ingestion timestamp from the Bronze filename.

    Example:
    vehicle_positions_20260912_105335.bin
    -> 2026-09-12 10:53:35 UTC
    """

    filename = os.path.basename(file_path)

    match = re.search(
        r"vehicle_positions_(\d{8})_(\d{6})\.bin",
        filename,
    )

    if not match:
        raise ValueError(
            f"Invalid Bronze filename: {filename}"
        )

    date_part = match.group(1)
    time_part = match.group(2)

    timestamp = datetime.strptime(
        f"{date_part}_{time_part}",
        "%Y%m%d_%H%M%S",
    )

    return timestamp.replace(
        tzinfo=timezone.utc
    )

def process_snapshot(file_path: str) -> dict:
    """Calculate metrics for one GTFS-Realtime snapshot."""

    with open(file_path, "rb") as file:
        raw_data = file.read()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_data)

    vehicles = set()
    routes = set()
    vehicle_records = 0

    vehicle_timestamps = []

    for entity in feed.entity:

        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle
        vehicle_records += 1

        if vehicle.HasField("vehicle"):
            vehicle_id = vehicle.vehicle.id

            if vehicle_id:
                vehicles.add(vehicle_id)

        if vehicle.HasField("trip"):
            route_id = vehicle.trip.route_id

            if route_id:
                routes.add(route_id)

        if vehicle.HasField("timestamp"):
            vehicle_timestamps.append(
                datetime.fromtimestamp(
                    vehicle.timestamp,
                    tz=timezone.utc,
                )
            )

    # Use the feed's vehicle timestamp when available.
    snapshot_time = (
        max(vehicle_timestamps)
        if vehicle_timestamps
        else None
    )

    # Our own ingestion timestamp.
    ingestion_time = get_ingestion_timestamp(
        file_path
    )

    return {
        "snapshot_time": snapshot_time,
        "ingestion_timestamp": ingestion_time,
        "source_file": os.path.basename(file_path),
        "active_vehicles": len(vehicles),
        "active_routes": len(routes),
        "total_records": vehicle_records,
    }


def build_snapshot_metrics(
    bronze_files: list[str],
) -> pd.DataFrame:

    records = []

    for index, file_path in enumerate(
        bronze_files,
        start=1,
    ):

        print(
            f"[{index}/{len(bronze_files)}] "
            f"Processing {os.path.basename(file_path)}"
        )

        metrics = process_snapshot(
            file_path
        )

        print(
            f"    Vehicles: "
            f"{metrics['active_vehicles']} | "
            f"Routes: "
            f"{metrics['active_routes']} | "
            f"Records: "
            f"{metrics['total_records']}"
        )

        records.append(metrics)

    return pd.DataFrame(records)


def save_gold(df: pd.DataFrame) -> None:

    os.makedirs(
        GOLD_DIR,
        exist_ok=True,
    )

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"\nGold table saved to: {OUTPUT_FILE}"
    )


def main():

    print("========================================")
    print("Creating Snapshot Metrics")
    print("========================================\n")

    bronze_files = get_bronze_files()

    print(
        f"Bronze snapshots: {len(bronze_files)}\n"
    )

    df = build_snapshot_metrics(
        bronze_files
    )

    # Sort chronologically.
    df = df.sort_values(
        "snapshot_time"
    ).reset_index(drop=True)

    save_gold(df)

    print("\nSnapshot metrics:\n")

    print(
        df.to_string(index=False)
    )


if __name__ == "__main__":
    main()