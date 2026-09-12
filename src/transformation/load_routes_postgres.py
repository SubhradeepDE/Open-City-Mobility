import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


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


def load_silver() -> pd.DataFrame:

    if not os.path.exists(SILVER_FILE):
        raise FileNotFoundError(
            f"Silver file not found: {SILVER_FILE}"
        )

    df = pd.read_parquet(
        SILVER_FILE
    )

    if df.empty:
        raise ValueError(
            "Silver dataset is empty."
        )

    return df


def build_routes(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df = df.dropna(
        subset=["route_id"]
    ).copy()

    df["vehicle_timestamp"] = pd.to_datetime(
        df["vehicle_timestamp"],
        utc=True,
        errors="coerce",
    )

    routes = (
        df.groupby("route_id")
        .agg(
            first_seen_at=(
                "vehicle_timestamp",
                "min",
            ),
            last_seen_at=(
                "vehicle_timestamp",
                "max",
            ),
            active_vehicle_count=(
                "vehicle_id",
                "nunique",
            ),
            active_trip_count=(
                "trip_id",
                "nunique",
            ),
        )
        .reset_index()
    )

    return routes


def load_into_postgres(
    routes: pd.DataFrame,
) -> None:

    connection = psycopg2.connect(
        **DB_CONFIG
    )

    cursor = connection.cursor()

    rows = [
        (
            str(row.route_id),
            row.first_seen_at.to_pydatetime(),
            row.last_seen_at.to_pydatetime(),
            int(row.active_vehicle_count),
            int(row.active_trip_count),
        )
        for row in routes.itertuples(
            index=False
        )
    ]

    sql = """
        INSERT INTO mobility.routes (
            route_id,
            first_seen_at,
            last_seen_at,
            active_vehicle_count,
            active_trip_count
        )
        VALUES %s
        ON CONFLICT (route_id)
        DO UPDATE SET
            first_seen_at = LEAST(
                mobility.routes.first_seen_at,
                EXCLUDED.first_seen_at
            ),
            last_seen_at = GREATEST(
                mobility.routes.last_seen_at,
                EXCLUDED.last_seen_at
            ),
            active_vehicle_count =
                EXCLUDED.active_vehicle_count,
            active_trip_count =
                EXCLUDED.active_trip_count;
    """

    execute_values(
        cursor,
        sql,
        rows,
    )

    connection.commit()

    cursor.close()
    connection.close()


def main():

    print("========================================")
    print("Loading Route Registry")
    print("========================================\n")

    df = load_silver()

    print(
        f"Silver records: {len(df)}"
    )

    routes = build_routes(df)

    print(
        f"Routes discovered: {len(routes)}"
    )

    load_into_postgres(routes)

    print(
        "Routes loaded into "
        "mobility.routes"
    )


if __name__ == "__main__":
    main()