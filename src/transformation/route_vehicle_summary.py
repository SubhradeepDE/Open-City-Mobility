import os

import pandas as pd


SILVER_FILE = "data/silver/vehicle_positions.parquet"
GOLD_DIR = "data/gold"


def create_route_vehicle_summary() -> pd.DataFrame:
    """Create a summary of active vehicles by route."""

    df = pd.read_parquet(SILVER_FILE)

    if df.empty:
        raise ValueError("Silver dataset is empty.")

    # Keep only records with the fields required for this metric.
    df = df.dropna(
        subset=[
            "vehicle_id",
            "route_id",
            "latitude",
            "longitude",
            "vehicle_timestamp",
        ]
    )

    # Each vehicle should be counted only once for this feed snapshot.
    latest_vehicle_positions = (
        df.sort_values("vehicle_timestamp")
        .drop_duplicates(
            subset=["vehicle_id"],
            keep="last",
        )
    )

    summary = (
        latest_vehicle_positions
        .groupby("route_id", as_index=False)
        .agg(
            active_vehicles=("vehicle_id", "nunique"),
            unique_trips=("trip_id", "nunique"),
        )
        .sort_values(
            "active_vehicles",
            ascending=False,
        )
    )

    return summary


def save_gold(summary: pd.DataFrame) -> str:
    """Save Gold table as Parquet."""

    os.makedirs(GOLD_DIR, exist_ok=True)

    output_file = os.path.join(
        GOLD_DIR,
        "route_vehicle_summary.parquet",
    )

    summary.to_parquet(
        output_file,
        index=False,
    )

    return output_file


def main():
    print("Creating Gold route summary...")

    summary = create_route_vehicle_summary()

    output_file = save_gold(summary)

    print(f"Gold table saved to: {output_file}")

    print("\nTop 10 routes by active vehicles:")
    print(summary.head(10).to_string(index=False))

    print(f"\nTotal active vehicles: {summary['active_vehicles'].sum()}")
    print(f"Total routes: {len(summary)}")


if __name__ == "__main__":
    main()