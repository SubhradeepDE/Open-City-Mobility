import sys
from datetime import datetime, timezone
from datetime import datetime, timezone
from src.quality.checks import check_not_empty
from src.quality.runner import all_checks_passed, run_checks
import psycopg2
from airflow.sdk import dag, task
import os


sys.path.insert(0, "/opt/airflow/project")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "mobility"),
    "user": os.getenv("DB_USER", "mobility_user"),
    "password": os.getenv("DB_PASSWORD", "mobility_password"),
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
        start_time = datetime.now(timezone.utc)
        print(
            f"🚦 Mobility pipeline started at "
            f"{datetime.now(timezone.utc).isoformat()}"
        )
        return start_time.isoformat()
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
        print("🔍 Checking route IDs using Data Quality framework...")

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT route_id
                FROM mobility.route_activity_summary
                """
            )

            rows = cursor.fetchall()

            checks = []

            for (route_id,) in rows:
                checks.append(
                    {
                        "name": "route_id_not_empty",
                        "function": check_not_empty,
                        "value": route_id,
                        "field_name": "route_id",
                    }
                )

            results = run_checks(checks)

            for result in results:
                print(result)

            if not all_checks_passed(results):
                raise RuntimeError(
                    "Data quality check failed: invalid route IDs found"
                )

            print(
                f"✅ Route ID data-quality check passed for "
                f"{len(rows)} records."
            )

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
    def collect_data_observability():
        print("📊 Collecting data observability metrics...")

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM mobility.vehicle_positions"
            )
            row_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM mobility.vehicle_positions
                WHERE vehicle_id IS NULL
                OR ingestion_timestamp IS NULL
                """
            )
            null_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT vehicle_id, vehicle_timestamp
                    FROM mobility.vehicle_positions
                    GROUP BY vehicle_id, vehicle_timestamp
                    HAVING COUNT(*) > 1
                ) duplicates
                """
            )
            duplicate_count = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO monitoring.data_quality_metrics (
                    table_name,
                    row_count,
                    null_count,
                    duplicate_count
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "mobility.vehicle_positions",
                    row_count,
                    null_count,
                    duplicate_count,
                ),
            )

            conn.commit()

            print(f"📦 Row count: {row_count}")
            print(f"⚠️ Null count: {null_count}")
            print(f"🔁 Duplicate count: {duplicate_count}")
            print("✅ Observability metrics saved.")

            cursor.close()

        finally:
            conn.close()

    @task
    def pipeline_completed(start_time):
        end_time = datetime.now(timezone.utc)

        start = datetime.fromisoformat(start_time)

        duration_seconds = (
            end_time - start
        ).total_seconds()

        print(
            f"✅ Mobility pipeline completed at "
            f"{end_time.isoformat()}"
        )

        print(
            f"⏱️ Pipeline duration: "
            f"{duration_seconds:.2f} seconds"
        )

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM mobility.route_activity_summary
                """
            )

            records_processed = cursor.fetchone()[0]

            print(
                f"📊 Records processed: "
                f"{records_processed}"
            )

            cursor.execute(
                """
                INSERT INTO monitoring.pipeline_runs (
                    pipeline_name,
                    status,
                    start_time,
                    end_time,
                    duration_seconds,
                    records_processed,
                    data_quality_status
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    "mobility_pipeline",
                    "SUCCESS",
                    start,
                    end_time,
                    duration_seconds,
                    records_processed,
                    "PASSED",
                ),
            )

            conn.commit()

            print(
                "✅ Pipeline monitoring record saved."
            )

            cursor.close()

        finally:
            conn.close()
    start_task = pipeline_started()
    refresh_task = refresh_route_summary()
    validate_task = validate_route_summary()
    quality_task = validate_route_ids()
    freshness_task = check_data_freshness()
    completed_task = pipeline_completed(start_task)
    observability_task = collect_data_observability()

    (
        start_task
        >> refresh_task
        >> validate_task
        >> quality_task
        >> freshness_task
        >> observability_task
        >> completed_task
    )


mobility_pipeline()