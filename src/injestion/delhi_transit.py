from datetime import datetime, timezone
import os
import requests
from dotenv import load_dotenv


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