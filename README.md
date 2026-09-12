# 🚦 Real Time City Mobility Traffic Intelligence Platform

> An open-source real-time city mobility and traffic intelligence platform for ingesting, processing, validating, and analyzing live public transportation data.

**Open City Mobility** is a community-driven Data Engineering project that turns real-time urban mobility data into a reusable data platform.

The project currently focuses on **Delhi public transit data** and is designed to eventually support multiple cities, mobility data sources, streaming pipelines, analytics, dashboards, alerts, and machine learning.

---

## 🎯 Project Vision

Cities continuously generate transportation data.

Buses move through different locations, routes become busy, services change, vehicles slow down, and mobility patterns change throughout the day.

The goal of this project is to build an open-source platform that continuously collects this data and transforms it into useful mobility intelligence.

### Long-term vision

```text
                Public Data Sources
                        │
                        ▼
                   Ingestion
                        │
                        ▼
                  Event Streaming
                        │
                        ▼
                     Bronze
                        │
                        ▼
                     Silver
                        │
                        ▼
                      Gold
                        │
                        ▼
             Analytics / Dashboard
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
            Alerts                ML
```

---

# 🚧 Project Status

**Early Development — MVP**

## ✅ Completed

### Data Ingestion

* Delhi Open Transit Data realtime API integration
* GTFS-Realtime vehicle position ingestion
* Continuous API polling
* Raw realtime feed storage

### Data Processing

* GTFS-Realtime protobuf decoding
* Bronze layer
* Silver layer
* Historical Silver processing
* GPS coordinate validation
* Timestamp validation
* Duplicate handling
* Observation-gap filtering

### Mobility Analytics

* Vehicle movement calculation
* Haversine distance calculation
* Speed estimation
* GPS speed anomaly detection
* Route performance analysis
* Hourly vehicle activity
* Snapshot-level mobility metrics

### Kafka Streaming

* Apache Kafka 4.x local deployment
* Kafka `vehicle_positions` topic
* 3 Kafka partitions
* Python Kafka producer
* Python Kafka consumer
* Kafka consumer groups
* Kafka offsets
* Consumer lag monitoring
* Kafka → Bronze consumer
* JSON event schema
* Event validation before publishing

### Local Development

* Docker Compose
* Kafka UI

---

## 🚧 Current Stage

The next major stage is:

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ▼
Streaming Silver
```

Planned work includes:

* Spark Structured Streaming
* Streaming transformations
* Event-time processing
* Watermarking
* Stateful processing
* Streaming data quality

---

# 📡 Current Data Source

The project currently uses **Delhi Open Transit Data (OTD)** for realtime public transportation data.

Realtime vehicle events contain information such as:

* Vehicle ID
* Route ID
* Trip ID
* Latitude
* Longitude
* Vehicle timestamp
* Vehicle status

Official source:

https://otd.delhi.gov.in/

---

# 🏗️ Architecture

## Historical Pipeline

```text
Delhi OTD API
      │
      ▼
Python Ingestion
      │
      ▼
🥉 Bronze
      │
      ▼
Decode + Clean
      │
      ▼
🥈 Silver
      │
      ▼
Mobility Analytics
      │
      ▼
🥇 Gold
```

## Realtime Pipeline

```text
Delhi OTD API
      │
      ▼
Python Producer
      │
      ▼
Schema Validation
      │
      ▼
Apache Kafka
      │
      ▼
vehicle_positions
      │
      ▼
Kafka Consumer
      │
      ▼
🥉 Streaming Bronze
```

## Target Streaming Architecture

```text
                 Delhi OTD API
                       │
                       ▼
                Python Producer
                       │
                       ▼
              ┌────────────────┐
              │ Apache Kafka   │
              │                │
              │vehicle_positions
              │ P0  P1  P2     │
              └───────┬────────┘
                      │
                      ▼
            Spark Structured Streaming
                      │
                      ▼
                  🥉 Bronze
                      │
                      ▼
                  🥈 Silver
                      │
                      ▼
                   🥇 Gold
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Dashboard    Alerts        ML
```

---

# 📁 Project Structure

```text
open-city-mobility/
│
├── data/
│   ├── bronze/
│   │   └── stream/
│   ├── silver/
│   ├── gold/
│   └── reference/
│
├── schemas/
│   └── vehicle_position.json
│
├── src/
│   │
│   ├── ingestion/
│   │   └── delhi_transit.py
│   │
│   ├── transformation/
│   │   ├── vehicle_positions.py
│   │   ├── build_historical_silver.py
│   │   ├── hourly_vehicle_activity.py
│   │   ├── vehicle_movement.py
│   │   ├── route_performance.py
│   │   └── snapshot_metrics.py
│   │
│   └── streaming/
│       ├── vehicle_producer.py
│       ├── vehicle_consumer.py
│       └── bronze_consumer.py
│
├── docs/
│
├── docker-compose.yml
├── README.md
├── LICENSE
└── .gitignore
```

---

# 🥉 Bronze Layer

Bronze stores data as close as possible to the original source.

Historical GTFS-Realtime feeds are stored as raw binary files.

Realtime Kafka events are persisted through the Kafka → Bronze consumer.

Example:

```text
data/bronze/
├── vehicle_positions_YYYYMMDD_HHMMSS.bin
└── stream/
    └── YYYY/MM/DD/
        └── vehicle_positions.jsonl
```

Bronze exists so that downstream data can be rebuilt without requesting the source again.

---

# 🥈 Silver Layer

Silver contains decoded, structured, and cleaned vehicle-position data.

Current schema:

| Column                | Description                            |
| --------------------- | -------------------------------------- |
| `vehicle_id`          | Unique vehicle identifier              |
| `route_id`            | Transit route identifier               |
| `trip_id`             | Trip identifier                        |
| `latitude`            | Vehicle latitude                       |
| `longitude`           | Vehicle longitude                      |
| `vehicle_timestamp`   | Timestamp reported by the vehicle feed |
| `ingestion_timestamp` | Timestamp associated with ingestion    |
| `vehicle_status`      | GTFS-Realtime vehicle status           |

Historical Silver dataset:

```text
data/silver/vehicle_positions_history.parquet
```

---

# 🥇 Gold Layer

The current Gold layer contains exploratory mobility analytics.

### Route Vehicle Summary

```text
route_vehicle_summary.parquet
```

Provides:

* Active vehicles by route
* Unique trips by route

### Hourly Vehicle Activity

```text
hourly_vehicle_activity.parquet
```

Provides:

* Active vehicles by hour
* Active routes by hour
* Total observations

### Vehicle Movement

```text
vehicle_movement.parquet
```

Provides:

* Distance between observations
* Time difference between observations
* Estimated speed
* Speed anomaly flag

### Route Performance

```text
route_performance.parquet
```

Provides:

* Active vehicles
* Movement observations
* Total observed distance
* Average speed
* Median speed
* Minimum speed
* Maximum speed

### Snapshot Metrics

```text
snapshot_metrics.parquet
```

Provides:

* Snapshot timestamp
* Ingestion timestamp
* Active vehicles
* Active routes
* Total records

> Gold analytics are currently exploratory because the project is still accumulating historical observations.

---

# 📨 Kafka

Apache Kafka is the realtime event transport layer.

## Topic

```text
vehicle_positions
```

Current development configuration:

```text
Partitions: 3
Replication factor: 1
```

Each vehicle position is published as an individual event.

Example:

```json
{
  "event_version": 1,
  "event_type": "vehicle_position",
  "vehicle_id": "DL1PD6470",
  "route_id": "2226",
  "trip_id": "TRIP123",
  "latitude": 28.60894,
  "longitude": 77.10159,
  "vehicle_timestamp": "2026-09-12T15:09:41Z",
  "ingestion_timestamp": "2026-09-12T15:09:42Z"
}
```

The producer uses `vehicle_id` as the Kafka message key.

---

# 📨 Kafka Producer

Location:

```text
src/streaming/vehicle_producer.py
```

Responsibilities:

```text
1. Fetch Delhi realtime feed
2. Decode GTFS-Realtime
3. Create vehicle events
4. Validate events
5. Publish events to Kafka
```

Pipeline:

```text
Delhi OTD
    │
    ▼
GTFS-Realtime
    │
    ▼
Python
    │
    ▼
JSON Event
    │
    ▼
Schema Validation
    │
    ▼
Kafka
```

---

# 📥 Kafka Consumer

Location:

```text
src/streaming/vehicle_consumer.py
```

Used for development and debugging.

It demonstrates:

* Kafka consumers
* Consumer groups
* Partitions
* Offsets
* Message consumption

---

# 🥉 Kafka → Bronze Consumer

Location:

```text
src/streaming/bronze_consumer.py
```

Responsibilities:

```text
Kafka
  ↓
Read event
  ↓
Attach Kafka metadata
  ↓
Persist raw event
  ↓
Streaming Bronze
```

Kafka metadata includes:

* Partition
* Offset
* Bronze ingestion timestamp

---

# 🧾 Event Schema

Vehicle-position events follow:

```text
schemas/vehicle_position.json
```

Current required fields include:

```text
event_version
event_type
vehicle_id
route_id
trip_id
latitude
longitude
vehicle_timestamp
ingestion_timestamp
```

The schema is versioned so the event contract can evolve safely.

---

# 🖥️ Kafka UI

Kafka UI is included for local development and debugging.

Start the platform:

```bash
docker compose up -d
```

Open:

```text
http://localhost:8080
```

Kafka UI can be used to inspect:

* Topics
* Messages
* Partitions
* Consumer groups
* Offsets
* Consumer lag

Primary topic:

```text
vehicle_positions
```

---

# 🐳 Local Development

Start infrastructure:

```bash
docker compose up -d
```

Check services:

```bash
docker compose ps
```

Stop infrastructure:

```bash
docker compose down
```

---

# 🔐 Configuration

Store secrets in environment variables.

Example:

```text
DELHI_TRANSIT_API_KEY=your_api_key
```

Never commit:

```text
.env
API keys
credentials
tokens
```

The API key must never appear in source code, Git history, logs, screenshots, or documentation.

---

# 🧠 Data Engineering Concepts

This project intentionally demonstrates practical Data Engineering concepts.

### Ingestion

* REST API ingestion
* GTFS-Realtime
* Continuous polling

### Streaming

* Apache Kafka
* Producers
* Consumers
* Topics
* Partitions
* Offsets
* Consumer groups
* Consumer lag

### Data Architecture

* Bronze
* Silver
* Gold
* Historical processing
* Streaming processing

### Data Quality

* Null handling
* Duplicate detection
* Coordinate validation
* Timestamp validation
* GPS anomaly detection
* Observation-gap filtering
* Schema validation

### Processing

* Python
* Pandas
* Parquet
* Protocol Buffers

### Mobility Analytics

* Vehicle activity
* Vehicle movement
* Distance estimation
* Speed estimation
* Route performance
* Time-series analysis

---

# 🌍 Future Data Sources

The platform is designed to eventually support:

```text
🚌 Public Transit
🌦 Weather
🚗 Traffic
✈️ Aviation
🚲 Bike Sharing
🚨 Service Alerts
📍 Geospatial Data
```

Each source should be implemented as an independent connector.

---

# 🏙️ Multi-City Vision

The long-term goal is to support multiple cities using a common platform.

```text
cities/
├── delhi/
├── mumbai/
├── bangalore/
├── london/
└── new_york/
```

Target architecture:

```text
                 Open City Mobility
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
        Delhi          Mumbai       Bangalore
          │              │              │
          └──────────────┼──────────────┘
                         ▼
                   Common Pipeline
```

A contributor should eventually be able to add a city without rewriting the complete pipeline.

---

# 🤝 Contributing

Open City Mobility is designed to be community-driven.

## Beginner Contributions

* Documentation
* Tests
* Bug fixes
* Configuration
* Examples

## Intermediate Contributions

* API connectors
* Streaming transformations
* Silver models
* Data-quality rules
* Dashboard components

## Advanced Contributions

* Spark Structured Streaming
* Kafka optimization
* Pipeline observability
* Anomaly detection
* Machine learning
* Cloud infrastructure
* Kubernetes

---

# 🗺️ Roadmap

## Phase 1 — MVP

* [x] Delhi realtime API ingestion
* [x] Bronze storage
* [x] Silver transformation
* [x] Historical processing
* [x] Initial mobility analytics

## Phase 2 — Kafka

* [x] Apache Kafka local deployment
* [x] `vehicle_positions` topic
* [x] Kafka producer
* [x] Kafka consumer
* [x] Consumer groups
* [x] Partition and offset tracking
* [x] Kafka → Bronze consumer
* [x] Event schema
* [x] Schema validation
* [x] Kafka UI

## Phase 3 — Spark Streaming

* [ ] Spark installation
* [ ] Spark Structured Streaming
* [ ] Kafka → Spark
* [ ] Streaming Bronze
* [ ] Streaming Silver
* [ ] Event-time processing
* [ ] Watermarking
* [ ] Stateful processing

## Phase 4 — Data Platform

* [ ] PostgreSQL serving layer
* [ ] Static GTFS reference data
* [ ] Route dimension
* [ ] Stop dimension
* [ ] Improved mobility models

## Phase 5 — Orchestration & Quality

* [ ] Airflow
* [ ] Automated data-quality framework
* [ ] CI/CD
* [ ] Pipeline monitoring

## Phase 6 — Analytics

* [ ] Live vehicle map
* [ ] Real-time dashboard
* [ ] Mobility anomaly detection
* [ ] Real-time alerts
* [ ] Historical comparisons

## Phase 7 — Community Expansion

* [ ] Multi-city framework
* [ ] Contributor guide
* [ ] Issue templates
* [ ] Pull-request templates
* [ ] Additional data connectors

## Phase 8 — Advanced Platform

* [ ] Data observability
* [ ] Mobility prediction
* [ ] Cloud deployment
* [ ] Terraform
* [ ] Kubernetes

---

# 📊 Dashboard

Planned dashboard capabilities:

* Live vehicle map
* Active vehicle count
* Active route count
* Route performance
* Vehicle movement
* Mobility trends
* Anomaly alerts
* Historical comparisons

### Dashboard Screenshot

Once the dashboard is available:

```markdown
![Dashboard](docs/images/dashboard.png)
```

---

# ⭐ Support the Project

If you find this project useful:

* ⭐ Star the repository
* 🐛 Report issues
* 💡 Suggest improvements
* 🔧 Submit pull requests
* 📖 Improve documentation
* 🌍 Add new cities or data sources

---

# 🚦 Final Goal

Transform:

```text
Raw public transportation feeds
```

into:

```text
A reusable open-source
real-time city mobility
intelligence platform.
```

**Built by the community. For the community.**
