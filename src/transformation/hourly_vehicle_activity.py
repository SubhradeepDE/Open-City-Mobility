import os

import pandas as pd


SILVER_FILE = "data/silver/vehicle_positions_history.parquet"
GOLD_DIR = "data/gold"
OUTPUT_FILE = os.path.join(
    GOLD_DIR,
    "hourly_vehicle_activity.parquet",
)


def load_silver_data() -> pd.DataFrame:
    """Load historical Silver vehicle positions."""

    if not os.path.exists(SILVER_FILE):
        raise FileNotFoundError(
            f"Silver file not found: {SILVER_FILE}"
        )

    df = pd.read_parquet(SILVER_FILE)

    if df.empty:
        raise ValueError("Silver dataset is empty.")

    return df


def create_hourly_activity(df: pd.DataFrame) -> pd.DataFrame:
    """Create hourly unique vehicle and route activity."""

    # Make sure timestamp is in UTC.
    df["vehicle_timestamp"] = pd.to_datetime(
        df["vehicle_timestamp"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=[
            "vehicle_timestamp",
            "vehicle_id",
        ]
    )

    # Convert every timestamp to the beginning of its hour.
    df["hour"] = df["vehicle_timestamp"].dt.floor("h")

    hourly = (
        df.groupby("hour")
        .agg(
            active_vehicles=("vehicle_id", "nunique"),
            active_routes=("route_id", "nunique"),
            total_records=("vehicle_id", "size"),
        )
        .reset_index()
        .sort_values("hour")
    )

    return hourly


def save_gold(df: pd.DataFrame) -> None:
    """Save hourly activity Gold table."""

    os.makedirs(GOLD_DIR, exist_ok=True)

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"Gold table saved to: {OUTPUT_FILE}"
    )


def main():
    print("========================================")
    print("Creating Hourly Vehicle Activity")
    print("========================================\n")

    df = load_silver_data()

    print(f"Silver records: {len(df)}")

    hourly = create_hourly_activity(df)

    save_gold(hourly)

    print("\nHourly activity:")
    print(
        hourly.to_string(index=False)
    )


if __name__ == "__main__":
    main()