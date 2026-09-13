from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="mobility_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["mobility", "postgres", "analytics"],
    default_args={
        "retries": 2,
    },
)
def mobility_pipeline():

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

        print("STDOUT:")
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
        import subprocess

        print("🔍 Validating route activity summary...")

        result = subprocess.run(
            [
                "python",
                "-c",
                """
                    import psycopg2
                        conn = psycopg2.connect(
                        host="postgres",
                        port=5432,
                        database="mobility",
                        user="mobility_user",
                        password="mobility_password",
                    )
                    )

                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM mobility.route_activity_summary")
                    count = cursor.fetchone()[0]

                    print(f"Rows in route_activity_summary: {count}")

                    if count == 0:
                        raise RuntimeError("Validation failed: route_activity_summary is empty")

                    cursor.close()
                    conn.close()
                    """,
            ],
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError("Route summary validation failed")

        print("✅ Route summary validation passed.")
    
    @task
    def validate_route_ids():
        import subprocess

        print("🔍 Checking route IDs...")

        result = subprocess.run(
            [
                "python",
                "-c",
                '''
                    import psycopg2
                    conn = psycopg2.connect(
                        host="postgres",
                        port=5432,
                        database="mobility",
                        user="mobility_user",
                        password="mobility_password",
                    )

                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM mobility.route_activity_summary
                        WHERE route_id IS NULL OR TRIM(route_id) = ''
                        """
                    )

                    invalid_count = cursor.fetchone()[0]

                    print(f"Invalid route IDs: {invalid_count}")

                    if invalid_count > 0:
                        raise RuntimeError(
                            f"Data quality check failed: {invalid_count} invalid route IDs found"
                        )

                    cursor.close()
                    conn.close()
                ''',
            ],
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError("Route ID data-quality check failed")

        print("✅ Route ID data-quality check passed.")

    refresh_task = refresh_route_summary()
    validate_task = validate_route_summary()
    quality_task = validate_route_ids()

    refresh_task >> validate_task >> quality_task

mobility_pipeline()