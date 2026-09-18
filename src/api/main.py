from datetime import datetime
from typing import Any
import os
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
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "mobility"),
    "user": os.getenv("DB_USER", "mobility_user"),
    "password": os.getenv("DB_PASSWORD", "mobility_password"),
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


@app.get("/vehicles/live")
def get_live_vehicles():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                vehicle_id,
                route_id,
                trip_id,
                latitude,
                longitude,
                vehicle_timestamp,
                ingestion_timestamp
            FROM mobility.latest_vehicle_positions
            ORDER BY updated_at DESC
            """
        )

        rows = cursor.fetchall()

        vehicles = []

        for row in rows:
            vehicles.append(
                {
                    "vehicle_id": row[0],
                    "route_id": row[1],
                    "trip_id": row[2],
                    "latitude": row[3],
                    "longitude": row[4],
                    "vehicle_timestamp": row[5].isoformat(),
                    "ingestion_timestamp": row[6].isoformat(),
                }
            )

        cursor.close()

        return {
            "count": len(vehicles),
            "vehicles": vehicles,
        }

    finally:
        conn.close()
@app.get("/analytics/anomalies")
def get_anomalies():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                route_id,
                hour,
                active_vehicles,
                avg_active_vehicles,
                deviation_percent,
                anomaly_status
            FROM mobility.route_activity_anomalies
            WHERE anomaly_status <> 'NORMAL'
            ORDER BY ABS(deviation_percent) DESC
            LIMIT 20
            """
        )

        rows = cursor.fetchall()

        anomalies = [
            {
                "route_id": row[0],
                "hour": row[1].isoformat() if row[1] else None,
                "active_vehicles": row[2],
                "avg_active_vehicles": float(row[3])
                if row[3] is not None
                else None,
                "deviation_percent": float(row[4])
                if row[4] is not None
                else None,
                "anomaly_status": row[5],
            }
            for row in rows
        ]

        cursor.close()

        return {
            "count": len(anomalies),
            "anomalies": anomalies,
        }

    finally:
        conn.close()

@app.get("/analytics/alerts")
def get_alerts():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                alert_id,
                route_id,
                alert_time,
                alert_type,
                deviation_percent,
                message
            FROM monitoring.mobility_alerts
            ORDER BY alert_id DESC
            LIMIT 20
            """
        )

        rows = cursor.fetchall()

        alerts = [
            {
                "alert_id": row[0],
                "route_id": row[1],
                "alert_time": row[2].isoformat()
                if row[2]
                else None,
                "alert_type": row[3],
                "deviation_percent": float(row[4])
                if row[4] is not None
                else None,
                "message": row[5],
            }
            for row in rows
        ]

        cursor.close()

        return {
            "count": len(alerts),
            "alerts": alerts,
        }

    finally:
        conn.close()