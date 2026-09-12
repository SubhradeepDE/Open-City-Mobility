import os

import pandas as pd


MOVEMENT_FILE = "data/gold/vehicle_movement.parquet"
OUTPUT_DIR = "data/gold"
OUTPUT_FILE = os.path.join(
    OUTPUT_DIR,
    "route_performance.parquet",
)


def load_movement_data() -> pd.DataFrame:
    """Load the vehicle movement Gold dataset."""

    if not os.path.exists(MOVEMENT_FILE):
        raise FileNotFoundError(
            f"Movement file not found: {MOVEMENT_FILE}"
        )

    df = pd.read_parquet(MOVEMENT_FILE)

    if df.empty:
        raise ValueError(
            "Vehicle movement dataset is empty."
        )

    return df


def create_route_performance(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Create route-level mobility metrics."""

    # Route is required for route-level analysis.
    df = df.dropna(
        subset=["route_id"]
    ).copy()

    # Ignore invalid/missing speed values.
    speed_df = df.dropna(
        subset=["speed_kmh"]
    ).copy()

    # Number of unique vehicles observed on each route.
    vehicle_counts = (
        df.groupby("route_id")["vehicle_id"]
        .nunique()
        .rename("active_vehicles")
    )

    # Movement-level statistics.
    performance = (
        speed_df.groupby("route_id")
        .agg(
            movement_records=(
                "vehicle_id",
                "size",
            ),
            total_distance_km=(
                "distance_km",
                "sum",
            ),
            average_speed_kmh=(
                "speed_kmh",
                "mean",
            ),
            median_speed_kmh=(
                "speed_kmh",
                "median",
            ),
            minimum_speed_kmh=(
                "speed_kmh",
                "min",
            ),
            maximum_speed_kmh=(
                "speed_kmh",
                "max",
            ),
        )
    )

    # Add unique vehicle count.
    performance = performance.join(
        vehicle_counts,
        how="left",
    )

    performance = (
        performance
        .reset_index()
        .sort_values(
            "average_speed_kmh",
            ascending=True,
        )
    )

    # ----------------------------------------------
    # Minimum sample-size requirement
    # ----------------------------------------------

    MIN_MOVEMENT_RECORDS = 5

    performance = performance[
    performance["movement_records"]
    >= MIN_MOVEMENT_RECORDS
    ].copy()

    return performance


def save_route_performance(
    df: pd.DataFrame,
) -> None:
    """Save route-level Gold table."""

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True,
    )

    df.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"Route performance saved to: "
        f"{OUTPUT_FILE}"
    )


def main():

    print("========================================")
    print("Creating Route Performance")
    print("========================================\n")

    movement = load_movement_data()

    print(
        f"Movement records: {len(movement)}"
    )

    performance = create_route_performance(
        movement
    )

    save_route_performance(
        performance
    )

    print("\nSlowest routes:\n")

    print(
        performance.head(10)
        .to_string(index=False)
    )

    print("\nFastest routes:\n")

    print(
        performance
        .sort_values(
            "average_speed_kmh",
            ascending=False,
        )
        .head(10)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()