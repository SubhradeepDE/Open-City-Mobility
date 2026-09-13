from datetime import datetime
from typing import Any

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(
    title="Open City Mobility API",
    description="API for real-time city mobility data and analytics",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "mobility",
    "user": "mobility_user",
    "password": "mobility_password",
}


@app.get("/health")
def health_check() -> dict[str, str]:
    return {
        "status": "healthy",
        "service": "open-city-mobility-api",
    }


@app.get("/vehicles/latest")
def get_latest_vehicles(
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
    ),
) -> dict[str, Any]:

    query = """
        SELECT
            vehicle_id,
            route_id,
            trip_id,
            latitude,
            longitude,
            vehicle_timestamp,
            ingestion_timestamp,
            kafka_partition,
            kafka_offset,
            updated_at
        FROM mobility.latest_vehicle_positions
        ORDER BY updated_at DESC
        LIMIT %s;
    """

    connection = None

    try:
        connection = psycopg2.connect(
            **DB_CONFIG
        )

        with connection.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()

            columns = [
                "vehicle_id",
                "route_id",
                "trip_id",
                "latitude",
                "longitude",
                "vehicle_timestamp",
                "ingestion_timestamp",
                "kafka_partition",
                "kafka_offset",
                "updated_at",
            ]

        vehicles = []

        for row in rows:
            record = dict(zip(columns, row))

            for field in [
                "vehicle_timestamp",
                "ingestion_timestamp",
                "updated_at",
            ]:
                value = record[field]

                if isinstance(value, datetime):
                    record[field] = value.isoformat()

            vehicles.append(record)

        return {
            "count": len(vehicles),
            "vehicles": vehicles,
        }

    except psycopg2.Error as error:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {error}",
        )

    finally:
        if connection is not None:
            connection.close()
@app.get("/routes/activity")
def get_route_activity(
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
    ),
) -> dict[str, Any]:

    query = """
        SELECT
            route_id,
            active_vehicle_count,
            active_trip_count,
            last_observed_at,
            updated_at
        FROM mobility.route_activity_summary
        ORDER BY active_vehicle_count DESC
        LIMIT %s;
    """

    connection = None

    try:
        connection = psycopg2.connect(
            **DB_CONFIG
        )

        with connection.cursor() as cursor:
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()

            columns = [
                "route_id",
                "active_vehicle_count",
                "active_trip_count",
                "last_observed_at",
                "updated_at",
            ]

        routes = []

        for row in rows:
            record = dict(zip(columns, row))

            for field in [
                "last_observed_at",
                "updated_at",
            ]:
                value = record[field]

                if isinstance(value, datetime):
                    record[field] = value.isoformat()

            routes.append(record)

        return {
            "count": len(routes),
            "routes": routes,
        }

    except psycopg2.Error as error:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {error}",
        )

    finally:
        if connection is not None:
            connection.close()
@app.get("/metrics/overview")
def get_metrics_overview() -> dict[str, Any]:

    query = """
        SELECT
            COUNT(*) AS total_vehicles,
            COUNT(DISTINCT route_id) AS active_routes,
            COUNT(DISTINCT trip_id) AS active_trips,
            MAX(vehicle_timestamp) AS latest_vehicle_timestamp
        FROM mobility.latest_vehicle_positions;
    """

    connection = None

    try:
        connection = psycopg2.connect(
            **DB_CONFIG
        )

        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()

        total_vehicles = row[0] or 0
        active_routes = row[1] or 0
        active_trips = row[2] or 0
        latest_vehicle_timestamp = row[3]

        if latest_vehicle_timestamp is not None:
            latest_vehicle_timestamp = (
                latest_vehicle_timestamp.isoformat()
            )

        return {
            "total_vehicles": total_vehicles,
            "active_routes": active_routes,
            "active_trips": active_trips,
            "latest_vehicle_timestamp":
                latest_vehicle_timestamp,
        }

    except psycopg2.Error as error:
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {error}",
        )

    finally:
        if connection is not None:
            connection.close()