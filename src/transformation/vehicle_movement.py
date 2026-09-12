import math
import os

import pandas as pd


SILVER_FILE = "data/silver/vehicle_positions_history.parquet"
GOLD_DIR = "data/gold"
OUTPUT_FILE = os.path.join(
    GOLD_DIR,
    "vehicle_movement.parquet",
)


# --------------------------------------------------
# Haversine distance
# --------------------------------------------------

def haversine_distance(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """
    Calculate distance between two GPS points.

    Returns:
        Distance in kilometers.
    """

    earth_radius_km = 6371.0

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)

    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad)
        * math.cos(lat2_rad)
        * math.sin(delta_lon / 2) ** 2
    )

    c = 2 * math.atan2(
        math.sqrt(a),
        math.sqrt(1 - a),
    )

    return earth_radius_km * c


# --------------------------------------------------
# Load Silver data
# --------------------------------------------------

def load_silver_data() -> pd.DataFrame:

    if not os.path.exists(SILVER_FILE):
        raise FileNotFoundError(
            f"Silver file not found: {SILVER_FILE}"
        )

    df = pd.read_parquet(SILVER_FILE)

    if df.empty:
        raise ValueError(
            "Silver dataset is empty."
        )

    return df


# --------------------------------------------------
# Calculate movement
# --------------------------------------------------

def calculate_movement(
    df: pd.DataFrame,
) -> pd.DataFrame:

    required_columns = [
        "vehicle_id",
        "route_id",
        "latitude",
        "longitude",
        "vehicle_timestamp",
    ]

    df = df.dropna(
        subset=required_columns
    ).copy()

    # Make sure timestamps are datetime.
    df["vehicle_timestamp"] = pd.to_datetime(
        df["vehicle_timestamp"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=["vehicle_timestamp"]
    )

    # Very important:
    # sort each vehicle's observations chronologically.
    df = df.sort_values(
        by=[
            "vehicle_id",
            "vehicle_timestamp",
        ]
    ).reset_index(drop=True)

    # Previous GPS position for THE SAME vehicle.
    df["previous_latitude"] = (
        df.groupby("vehicle_id")["latitude"]
        .shift(1)
    )

    df["previous_longitude"] = (
        df.groupby("vehicle_id")["longitude"]
        .shift(1)
    )

    df["previous_timestamp"] = (
        df.groupby("vehicle_id")["vehicle_timestamp"]
        .shift(1)
    )

    # Remove first observation of every vehicle.
    # There is no previous location for it.
    df = df.dropna(
        subset=[
            "previous_latitude",
            "previous_longitude",
            "previous_timestamp",
        ]
    ).copy()

    # ----------------------------------------------
    # Remove observations with large time gaps
    # ----------------------------------------------

    MAX_GAP_MINUTES = 5

    df = df[
        (
            df["vehicle_timestamp"]
            - df["previous_timestamp"]
        ).dt.total_seconds() / 60
        <= MAX_GAP_MINUTES
    ].copy()

    # ----------------------------------------------
    # Calculate distance
    # ----------------------------------------------

    df["distance_km"] = df.apply(
        lambda row: haversine_distance(
            row["previous_latitude"],
            row["previous_longitude"],
            row["latitude"],
            row["longitude"],
        ),
        axis=1,
    )

    # ----------------------------------------------
    # Calculate time difference
    # ----------------------------------------------

    df["time_difference_minutes"] = (
        (
            df["vehicle_timestamp"]
            - df["previous_timestamp"]
        )
        .dt.total_seconds()
        / 60
    )

    # Don't calculate speed where time is zero/negative.
    df.loc[
        df["time_difference_minutes"] <= 0,
        "time_difference_minutes",
    ] = None

    # ----------------------------------------------
    # Calculate speed
    # ----------------------------------------------

    df["speed_kmh"] = (
        df["distance_km"]
        / (df["time_difference_minutes"] / 60)
    )

    # ----------------------------------------------
    # GPS sanity check
    # ----------------------------------------------
    #
    # A bus cannot realistically travel at
    # hundreds of km/h inside the city.
    #
    # We will mark suspicious observations
    # instead of silently deleting them.

    df["is_speed_anomaly"] = (
        df["speed_kmh"] > 120
    )

    # For our initial city-mobility analysis,
    # don't use impossible speeds.

    df.loc[
        df["is_speed_anomaly"],
        "speed_kmh",
    ] = None

    return df[
        [
            "vehicle_id",
            "route_id",
            "trip_id",
            "latitude",
            "longitude",
            "vehicle_timestamp",
            "distance_km",
            "time_difference_minutes",
            "speed_kmh",
            "is_speed_anomaly",
        ]
    ]


# --------------------------------------------------
# Save Gold table
# --------------------------------------------------

def save_gold(
    df: pd.DataFrame,
) -> None:

    os.makedirs(
        GOLD_DIR,
        exist_ok=True,
    )

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"Gold table saved to: {OUTPUT_FILE}"
    )


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    print("========================================")
    print("Creating Vehicle Movement Gold Table")
    print("========================================\n")

    df = load_silver_data()

    print(
        f"Silver records: {len(df)}"
    )

    movement = calculate_movement(df)

    print(
        f"Movement records: {len(movement)}"
    )

    print(
        f"Speed anomalies: "
        f"{movement['is_speed_anomaly'].sum()}"
    )

    print(
        f"Total distance observed: "
        f"{movement['distance_km'].sum():.2f} km"
    )

    print(
        f"Average speed: "
        f"{movement['speed_kmh'].mean():.2f} km/h"
    )

    save_gold(movement)

    print("\nSample movement data:\n")

    print(
        movement.head(10)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()