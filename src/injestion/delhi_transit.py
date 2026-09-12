from datetime import datetime, timezone
import os
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2


load_dotenv()

API_KEY = os.getenv("DELHI_TRANSIT_API_KEY")

if not API_KEY:
    raise ValueError("DELHI_TRANSIT_API_KEY is not set")

# Use the VehiclePositions endpoint from the Delhi OTD documentation.
URL = f"https://otd.delhi.gov.in/api/realtime/VehiclePositions.pb?key={API_KEY}"

headers = {
    "Authorization": f"Bearer {API_KEY}"
}

response = requests.get(URL, headers=headers, timeout=30)
response.raise_for_status()

timestamp = datetime.now(timezone.utc)
print("Status code:", response.status_code)

filename = f"data/bronze/vehicle_positions_{timestamp.strftime('%Y%m%d_%H%M%S')}.bin"

with open(filename, "wb") as file:
    file.write(response.content)

print(f"Raw data saved to: {filename}")

# --------------------------------------------------
# Decode GTFS-Realtime protobuf data
# --------------------------------------------------

feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)

print(f"Total entities received: {len(feed.entity)}")

# --------------------------------------------------
# Read vehicle positions
# --------------------------------------------------

vehicle_count = 0

for entity in feed.entity:

    if not entity.HasField("vehicle"):
        continue

    vehicle = entity.vehicle

    vehicle_id = (
        vehicle.vehicle.id
        if vehicle.HasField("vehicle")
        else None
    )

    route_id = (
        vehicle.trip.route_id
        if vehicle.HasField("trip")
        else None
    )

    latitude = (
        vehicle.position.latitude
        if vehicle.HasField("position")
        else None
    )

    longitude = (
        vehicle.position.longitude
        if vehicle.HasField("position")
        else None
    )

    vehicle_timestamp = (
        vehicle.timestamp
        if vehicle.HasField("timestamp")
        else None
    )

    print(
        f"Vehicle: {vehicle_id} | "
        f"Route: {route_id} | "
        f"Lat: {latitude} | "
        f"Lon: {longitude} | "
        f"Timestamp: {vehicle_timestamp}"
    )

    vehicle_count += 1

print(f"\nVehicle positions found: {vehicle_count}")