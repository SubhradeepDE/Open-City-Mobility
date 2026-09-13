import math
import pandas as pd
import psycopg2


SILVER_FILE = (
    "data/silver/vehicle_positions_history.parquet"
)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "mobility",
    "user": "mobility_user",
    "password": "mobility_password",
}


# Approximately 100-150 meters depending on latitude.
GRID_DECIMAL_PLACES = 3


def load_data() -> pd.DataFrame:

    df = pd.read_parquet(SILVER_FILE)

    required = [
        "vehicle_id",
        "latitude",
        "longitude",
        "vehicle_timestamp",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing columns: {missing}"
        )

    df = df.dropna(
        subset=[
            "latitude",
            "longitude",
            "vehicle_timestamp",
        ]
    ).copy()

    df["vehicle_timestamp"] = pd.to_datetime(
        df["vehicle_timestamp"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=["vehicle_timestamp"]
    )

    return df


def build_inferred_stops(
    df: pd.DataFrame,
) -> pd.DataFrame:

    # Group nearby observations into spatial cells.
    #
    # This is an inferred mobility point,
    # NOT an official transit stop.

    df["lat_cell"] = df["latitude"].round(
        GRID_DECIMAL_PLACES
    )

    df["lon_cell"] = df["longitude"].round(
        GRID_DECIMAL_PLACES
    )

    grouped = (
        df.groupby(
            ["lat_cell", "lon_cell"]
        )
        .agg(
            observation_count=(
                "vehicle_id",
                "count",
            ),
            first_seen_at=(
                "vehicle_timestamp",
                "min",
            ),
            last_seen_at=(
                "vehicle_timestamp",
                "max",
            ),
            unique_vehicles=(
                "vehicle_id",
                "nunique",
            ),
        )
        .reset_index()
    )

    # A location becomes an inferred stop only when
    # it has enough repeated observations.
    grouped = grouped[
        grouped["observation_count"] >= 10
    ].copy()

    grouped["latitude"] = grouped[
        "lat_cell"
    ]

    grouped["longitude"] = grouped[
        "lon_cell"
    ]

    grouped["stop_id"] = (
        "INF_"
        + grouped["lat_cell"].astype(str)
        + "_"
        + grouped["lon_cell"].astype(str)
    )

    grouped["stop_name"] = (
        "Inferred Mobility Point "
        + grouped["stop_id"]
    )

    grouped["location_type"] = 0
    grouped["source"] = "realtime_inferred"

    return grouped[
        [
            "stop_id",
            "stop_name",
            "latitude",
            "longitude",
            "location_type",
            "source",
            "observation_count",
            "first_seen_at",
            "last_seen_at",
        ]
    ]


def load_into_postgres(
    stops: pd.DataFrame,
):

    connection = psycopg2.connect(
        **DB_CONFIG
    )

    try:

        with connection.cursor() as cursor:

            sql = """
                INSERT INTO mobility.stops (
                    stop_id,
                    stop_name,
                    stop_lat,
                    stop_lon,
                    location_type,
                    source,
                    observation_count,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (stop_id)
                DO UPDATE SET
                    stop_name =
                        EXCLUDED.stop_name,
                    stop_lat =
                        EXCLUDED.stop_lat,
                    stop_lon =
                        EXCLUDED.stop_lon,
                    location_type =
                        EXCLUDED.location_type,
                    source =
                        EXCLUDED.source,
                    observation_count =
                        EXCLUDED.observation_count,
                    first_seen_at =
                        EXCLUDED.first_seen_at,
                    last_seen_at =
                        EXCLUDED.last_seen_at;
            """

            for row in stops.itertuples(
                index=False
            ):

                cursor.execute(
                    sql,
                    (
                        row.stop_id,
                        row.stop_name,
                        float(row.latitude),
                        float(row.longitude),
                        int(row.location_type),
                        row.source,
                        int(row.observation_count),
                        row.first_seen_at.to_pydatetime(),
                        row.last_seen_at.to_pydatetime(),
                    ),
                )

        connection.commit()

    finally:
        connection.close()


def main():

    print("========================================")
    print("Building Inferred Stop Dimension")
    print("========================================")

    df = load_data()

    print(
        f"Input observations: {len(df)}"
    )

    stops = build_inferred_stops(df)

    print(
        f"Inferred mobility points: {len(stops)}"
    )

    load_into_postgres(stops)

    print(
        "Inferred stops loaded into "
        "mobility.stops"
    )


if __name__ == "__main__":
    main()