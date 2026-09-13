from datetime import datetime, timezone

import psycopg2
from airflow.sdk import dag, task


DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "mobility",
    "user": "mobility_user",
    "password": "mobility_password",
}


@dag(
    dag_id="mobility_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["mobility", "postgres", "quality"],
    default_args={
        "retries": 2,
    },
)
def mobility_pipeline():

    @task
    def pipeline_started():
        print(
            f"🚦 Mobility pipeline started at "
            f"{datetime.now(timezone.utc).isoformat()}"
        )

    @task
    def refresh_route_summary():
        import subprocess

        print("🚦 Starting route activity summary refresh...")

        result = subprocess.run(
            [
                "python",
                "/opt/airflow/project/src/transformation/route_activity_summary.py",
            ],
            capture_output=True,
            text=True,
        )

        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print("STDERR:")
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"Route summary refresh failed with exit code {result.returncode}"
            )

        print("✅ Route activity summary refreshed successfully.")

    @task
    def validate_route_summary():
        print("🔍 Validating route activity summary...")

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM mobility.route_activity_summary"
            )

            count = cursor.fetchone()[0]

            print(f"Rows in route_activity_summary: {count}")

            if count == 0:
                raise RuntimeError(
                    "Validation failed: route_activity_summary is empty"
                )

            print("✅ Route summary validation passed.")

            cursor.close()
        finally:
            conn.close()

    @task
    def validate_route_ids():
        print("🔍 Checking route IDs...")

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM mobility.route_activity_summary
                WHERE route_id IS NULL
                   OR TRIM(route_id) = ''
                """
            )

            invalid_count = cursor.fetchone()[0]

            print(f"Invalid route IDs: {invalid_count}")

            if invalid_count > 0:
                raise RuntimeError(
                    f"Data quality check failed: "
                    f"{invalid_count} invalid route IDs found"
                )

            print("✅ Route ID data-quality check passed.")

            cursor.close()
        finally:
            conn.close()

    @task
    def check_data_freshness():
        print("⏱️ Checking vehicle data freshness...")

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT MAX(ingestion_timestamp)
                FROM mobility.vehicle_positions
                """
            )

            latest_timestamp = cursor.fetchone()[0]

            print(f"Latest ingestion timestamp: {latest_timestamp}")

            if latest_timestamp is None:
                print("⚠️ No vehicle data found.")
                return

            if latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.replace(
                    tzinfo=timezone.utc
                )

            now = datetime.now(timezone.utc)

            age_minutes = (
                now - latest_timestamp
            ).total_seconds() / 60

            print(f"Data age: {age_minutes:.2f} minutes")

            if age_minutes > 10:
                print(
                    f"⚠️ Data is stale by {age_minutes:.2f} minutes. "
                    "Realtime ingestion may currently be stopped."
                )
                return

            print("✅ Data freshness check passed.")

            cursor.close()

        finally:
            conn.close()

    @task
    def pipeline_completed():
        print(
            f"✅ Mobility pipeline completed successfully at "
            f"{datetime.now(timezone.utc).isoformat()}"
        )

    start_task = pipeline_started()
    refresh_task = refresh_route_summary()
    validate_task = validate_route_summary()
    quality_task = validate_route_ids()
    freshness_task = check_data_freshness()
    completed_task = pipeline_completed()

    (
        start_task
        >> refresh_task
        >> validate_task
        >> quality_task
        >> freshness_task
        >> completed_task
    )


mobility_pipeline()