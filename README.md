# 🚦 Open City Mobility

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

* Apache Kafka 4.0.2 local deployment
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

### PySpark Streaming

* PySpark 4.2.0 installation
* Spark Structured Streaming
* Kafka → PySpark
* Kafka JSON parsing
* Streaming Silver Parquet pipeline
* Event-time processing
* Watermarking
* Streaming data-quality validation
* Invalid-event quarantine

### Local Development

* Docker Compose
* Apache Kafka
* Kafka UI

---

## 🚧 Current Stage

The project is currently completing the final part of the initial Spark Streaming phase:

```text
Kafka
  │
  ▼
PySpark Structured Streaming
  │
  ▼
Event-Time Processing
  │
  ▼
Watermarking
  │
  ▼
Stateful Processing
```

**Stateful processing is currently under development and has not yet been marked complete until the stateful output is successfully validated.**

The next stages after that are:

```text
Streaming Platform
        ↓
Data Platform
        ↓
PostgreSQL
        ↓
Reference / Dimension Data
        ↓
Dashboard
```

---

# 📡 Current Data Source

The project currently uses **Delhi Open Transit Data (OTD)** for realtime public transportation data.

Realtime vehicle events include information such as:

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

## PySpark Streaming Pipeline

```text
                Apache Kafka
                     │
                     ▼
             vehicle_positions
                     │
                     ▼
        PySpark Structured Streaming
                     │
                     ▼
                Parse JSON
                     │
                     ▼
               Data Quality
                /        \
               /          \
              ▼            ▼
           Valid          Invalid
              │              │
              ▼              ▼
       Streaming Silver   Quarantine
```

## Target Architecture

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
            PySpark Structured Streaming
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
│   │   ├── streaming/
│   │   └── stateful_vehicle_positions/
│   ├── gold/
│   ├── quarantine/
│   │   └── vehicle_positions/
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
│       ├── bronze_consumer.py
│       ├── pyspark_kafka.py
│       ├── pyspark_vehicle_parser.py
│       ├── pyspark_vehicle_silver.py
│       ├── pyspark_vehicle_quality.py
│       ├── pyspark_event_time.py
│       └── stateful_vehicle_tracking.py
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

The Bronze layer stores data as close as possible to the original source.

Historical GTFS-Realtime feeds are stored as raw binary files.

Realtime Kafka events are persisted through the Kafka → Bronze consumer as JSONL data.

Example:

```text
data/bronze/
├── vehicle_positions_YYYYMMDD_HHMMSS.bin
└── stream/
    └── YYYY/MM/DD/
        └── vehicle_positions.jsonl
```

Bronze exists so downstream data can be rebuilt without requesting the source again.

---

# 🥈 Silver Layer

The Silver layer contains decoded, structured, validated, and cleaned vehicle-position data.

Current schema includes:

| Column                | Description                            |
| --------------------- | -------------------------------------- |
| `event_version`       | Event schema version                   |
| `event_type`          | Type of realtime event                 |
| `vehicle_id`          | Unique vehicle identifier              |
| `route_id`            | Transit route identifier               |
| `trip_id`             | Trip identifier                        |
| `latitude`            | Vehicle latitude                       |
| `longitude`           | Vehicle longitude                      |
| `vehicle_timestamp`   | Timestamp reported by the vehicle feed |
| `ingestion_timestamp` | Timestamp associated with ingestion    |
| `kafka_partition`     | Kafka source partition                 |
| `kafka_offset`        | Kafka source offset                    |
| `kafka_timestamp`     | Kafka record timestamp                 |

Historical Silver:

```text
data/silver/vehicle_positions_history.parquet
```

Streaming Silver:

```text
data/silver/streaming/
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

Current local development configuration:

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

# 🔥 PySpark Streaming

The project uses **PySpark 4.2.0** for Spark Structured Streaming.

## Kafka → PySpark

```text
Kafka
  ↓
spark.readStream
  ↓
vehicle_positions
  ↓
Raw Kafka DataFrame
```

## JSON Parsing

Kafka values are parsed using a defined Spark schema:

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

## Streaming Silver

Valid events are transformed into structured Silver data and written to:

```text
data/silver/streaming/
```

## Event Time

`vehicle_timestamp` is treated as the event-time field.

This is separate from:

```text
Kafka timestamp
```

and:

```text
ingestion timestamp
```

This distinction is important for late and out-of-order events.

## Watermarking

The current event-time demonstration uses a:

```text
2 minute watermark
```

to handle late-arriving events.

## Stateful Processing

Stateful vehicle tracking is currently under development.

The intended state key is:

```text
vehicle_id
```

The intended state contains:

```text
latest route
latest trip
latest latitude
latest longitude
latest vehicle timestamp
```

The stateful pipeline is **not yet marked complete** until its output is validated successfully.

---

# 🧾 Event Schema

Vehicle-position events follow:

```text
schemas/vehicle_position.json
```

Current event contract includes:

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

The event schema is versioned to allow controlled schema evolution.

---

# 🚨 Data Quality & Quarantine

Streaming validation checks include:

```text
Event version
Event type
Vehicle ID
Latitude
Longitude
Vehicle timestamp
Ingestion timestamp
```

Invalid records are routed separately rather than silently discarded.

```text
                    Kafka
                      │
                      ▼
                   PySpark
                      │
                 Validation
                 /         \
                /           \
             VALID         INVALID
                │              │
                ▼              ▼
             Silver       Quarantine
```

Quarantine location:

```text
data/quarantine/vehicle_positions/
```

This allows failed events to be inspected and debugged later.

---

# 🖥️ Kafka UI

Kafka UI is included for local Kafka development and debugging.

Start the environment:

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

# ⚙️ Running the Streaming Components

## Start Kafka

```bash
docker compose up -d
```

## Start the producer

```bash
python src/streaming/vehicle_producer.py
```

## Run the basic Kafka consumer

```bash
python src/streaming/vehicle_consumer.py
```

## Run the PySpark Kafka parser

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  src/streaming/pyspark_vehicle_parser.py
```

## Run the Streaming Silver pipeline

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  src/streaming/pyspark_vehicle_silver.py
```

## Run the event-time pipeline

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  src/streaming/pyspark_event_time.py
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

The API key must never appear in:

* Source code
* Git history
* Logs
* Documentation
* Screenshots

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

### Spark

* PySpark
* Structured Streaming
* Kafka integration
* Streaming DataFrames
* Event-time processing
* Watermarking
* Stateful processing

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
* Invalid-event quarantine

### Processing

* Python
* Pandas
* PySpark
* Parquet
* Protocol Buffers
* JSON

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
* [x] 3 Kafka partitions
* [x] Kafka producer
* [x] Kafka consumer
* [x] Consumer groups
* [x] Partition and offset tracking
* [x] Consumer lag monitoring
* [x] Kafka → Bronze consumer
* [x] Event schema
* [x] Schema validation
* [x] Kafka UI

## Phase 3 — Spark Streaming

* [x] PySpark installation
* [x] Spark Structured Streaming
* [x] Kafka → Spark
* [x] Streaming Bronze
* [x] Streaming Silver
* [x] Event-time processing
* [x] Watermarking
* [ ] Stateful processing

## Phase 4 — Data Platform

* [ ] PostgreSQL serving layer
* [ ] Static GTFS reference data
* [ ] Route dimension
* [ ] Stop dimension
* [ ] Improved mobility data model
* [ ] Analytical serving layer

## Phase 5 — Orchestration & Quality

* [ ] Airflow
* [ ] Automated data-quality framework
* [ ] CI/CD
* [ ] Pipeline monitoring
* [ ] Data lineage
* [ ] Data observability

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

* [ ] Mobility prediction
* [ ] Machine learning
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

Once the dashboard is available, add the screenshot here:

```markdown
![Dashboard](docs/images/dashboard.png)
```

---

# 📄 License

See [`LICENSE`](LICENSE) for license information.

---

# ⭐ Support the Project

If you find the project useful:

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
