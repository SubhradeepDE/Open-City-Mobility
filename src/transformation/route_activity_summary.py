import psycopg2


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "mobility",
    "user": "mobility_user",
    "password": "mobility_password",
}


def refresh_route_activity_summary():
    connection = psycopg2.connect(**DB_CONFIG)

    try:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO mobility.route_activity_summary (
                    route_id,
                    active_vehicle_count,
                    active_trip_count,
                    last_observed_at,
                    updated_at
                )
                SELECT
                    route_id,
                    COUNT(DISTINCT vehicle_id),
                    COUNT(DISTINCT trip_id),
                    MAX(vehicle_timestamp),
                    NOW()
                FROM mobility.latest_vehicle_positions
                WHERE route_id IS NOT NULL
                GROUP BY route_id
                ON CONFLICT (route_id)
                DO UPDATE SET
                    active_vehicle_count =
                        EXCLUDED.active_vehicle_count,
                    active_trip_count =
                        EXCLUDED.active_trip_count,
                    last_observed_at =
                        EXCLUDED.last_observed_at,
                    updated_at =
                        NOW();
                """
            )

            connection.commit()

    finally:
        connection.close()


def main():
    print("========================================")
    print("Refreshing Route Activity Summary")
    print("========================================")

    refresh_route_activity_summary()

    print(
        "Route activity summary refreshed."
    )


if __name__ == "__main__":
    main()