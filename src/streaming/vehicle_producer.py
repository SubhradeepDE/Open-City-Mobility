import json
import os
import time
from datetime import datetime, timezone
from jsonschema import ValidationError, validate

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer


# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("DELHI_TRANSIT_API_KEY")

if not API_KEY:
    raise ValueError(
        "DELHI_TRANSIT_API_KEY is not set"
    )

API_URL = f"https://otd.delhi.gov.in/api/realtime/VehiclePositions.pb?key={API_KEY}"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"

POLL_INTERVAL_SECONDS = 60

SCHEMA_FILE = "schemas/vehicle_position.json"

with open(SCHEMA_FILE, "r", encoding="utf-8") as file:
    VEHICLE_POSITION_SCHEMA = json.load(file)

# --------------------------------------------------
# Kafka Producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    # Convert Python dictionaries into JSON bytes.
    value_serializer=lambda value: json.dumps(
        value
    ).encode("utf-8"),

    # Wait for acknowledgement from Kafka.
    acks="all",

    # Retry temporary failures.
    retries=3,
)


# --------------------------------------------------
# Get realtime feed
# --------------------------------------------------

def fetch_vehicle_feed():
    """Fetch the latest Delhi GTFS-Realtime feed."""

    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    response = requests.get(
        API_URL,
        headers=headers,
        timeout=30,
    )

    response.raise_for_status()

    return response.content

def validate_vehicle_event(event: dict) -> bool:
    """Validate an event against the vehicle position schema."""

    try:
        validate(
            instance=event,
            schema=VEHICLE_POSITION_SCHEMA,
        )
        return True

    except ValidationError as error:
        print(
            f"Invalid vehicle event: "
            f"{error.message}"
        )
        return False

# --------------------------------------------------
# Decode GTFS-Realtime
# --------------------------------------------------

def decode_vehicle_positions(
    raw_data: bytes,
) -> list[dict]:
    """Convert GTFS-Realtime protobuf into dictionaries."""

    feed = gtfs_realtime_pb2.FeedMessage()

    feed.ParseFromString(raw_data)

    ingestion_timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    vehicles = []

    for entity in feed.entity:

        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle

        vehicle_id = None
        route_id = None
        trip_id = None
        latitude = None
        longitude = None
        vehicle_timestamp = None

        # Vehicle ID
        if vehicle.HasField("vehicle"):
            vehicle_id = vehicle.vehicle.id

        # Trip information
        if vehicle.HasField("trip"):

            route_id = (
                vehicle.trip.route_id
            )

            trip_id = (
                vehicle.trip.trip_id
            )

        # Position
        if vehicle.HasField("position"):

            latitude = (
                vehicle.position.latitude
            )

            longitude = (
                vehicle.position.longitude
            )

        # Vehicle timestamp
        if vehicle.HasField("timestamp"):

            vehicle_timestamp = (
                datetime.fromtimestamp(
                    vehicle.timestamp,
                    tz=timezone.utc,
                ).isoformat()
            )

        # Ignore records without the minimum
        # information required by our event schema.
        if not vehicle_id:
            continue

        event = {
            "event_version": 1,
            "event_type": "vehicle_position",
            "vehicle_id": vehicle_id,
            "route_id": route_id,
            "trip_id": trip_id,
            "latitude": latitude,
            "longitude": longitude,
            "vehicle_timestamp": vehicle_timestamp,
            "ingestion_timestamp": ingestion_timestamp,
        }

        vehicles.append(event)

    return vehicles


# --------------------------------------------------
# Publish events to Kafka
# --------------------------------------------------

def publish_vehicle_events(
    vehicles: list[dict],
):
    """Publish vehicle events to Kafka."""

    successful = 0

    for vehicle in vehicles:

        if not validate_vehicle_event(vehicle):
            continue

        future = producer.send(
            KAFKA_TOPIC,
            key=vehicle["vehicle_id"].encode("utf-8"),
            value=vehicle,
        )

        # Wait for Kafka acknowledgement.
        future.get(timeout=10)

        successful += 1

    # Ensure buffered messages are sent.
    producer.flush()

    return successful


# --------------------------------------------------
# Main loop
# --------------------------------------------------

def main():

    print("========================================")
    print("Delhi Mobility Kafka Producer")
    print("========================================")

    print(
        f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic: {KAFKA_TOPIC}"
    )

    print(
        f"Polling every "
        f"{POLL_INTERVAL_SECONDS} seconds"
    )

    print()

    try:

        while True:

            cycle_start = datetime.now(
                timezone.utc
            )

            print(
                f"[{cycle_start.isoformat()}] "
                f"Fetching vehicle data..."
            )

            try:

                raw_data = fetch_vehicle_feed()

                vehicles = (
                    decode_vehicle_positions(
                        raw_data
                    )
                )

                print(
                    f"Vehicles received: "
                    f"{len(vehicles)}"
                )

                published = (
                    publish_vehicle_events(
                        vehicles
                    )
                )

                print(
                    f"Events published: "
                    f"{published}"
                )

            except Exception as error:

                print(
                    f"ERROR: {error}"
                )

            print(
                f"Waiting "
                f"{POLL_INTERVAL_SECONDS} seconds...\n"
            )

            time.sleep(
                POLL_INTERVAL_SECONDS
            )

    except KeyboardInterrupt:

        print(
            "\nStopping producer..."
        )

    finally:

        producer.flush()
        producer.close()

        print(
            "Kafka producer closed."
        )


if __name__ == "__main__":
    main()