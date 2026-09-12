# 🚦 Real Time City Mobility Traffic Intelligence Platform

> An open-source real-time city mobility and traffic intelligence platform built to ingest, process, validate, and analyze live public transportation data.

The goal of **Open City Mobility** is to build a production-style Data Engineering platform around real-time urban mobility data.

The platform currently focuses on **Delhi NCR public transit data** and is designed so contributors can later add new cities, data sources, transformations, dashboards, and analytics.

---

## 🎯 Project Vision

Cities generate huge amounts of transportation data every day.

Buses move across the city, routes become busy, vehicles slow down, services change, and weather can affect mobility.

This project brings those different signals into a single open-source data platform.

### The long-term vision

```text
Public Data Sources
        ↓
    Ingestion
        ↓
   Event Streaming
        ↓
      Bronze
        ↓
      Silver
        ↓
       Gold
        ↓
 Data Quality & Monitoring
        ↓
 Analytics / Dashboard / Alerts
```

The platform is designed to eventually support multiple cities and multiple mobility data sources.

---

# 🚧 Current Project Status

**Early Development — MVP**

### Currently implemented

* ✅ Delhi Open Transit Data real-time API ingestion
* ✅ GTFS-Realtime vehicle position decoding
* ✅ Raw Bronze data storage
* ✅ Silver-layer vehicle position transformation
* ✅ Historical vehicle position processing
* ✅ Vehicle movement calculation using GPS coordinates
* ✅ Haversine distance calculation
* ✅ Speed calculation
* ✅ GPS speed anomaly detection
* ✅ Route-level performance analysis
* ✅ Snapshot-level mobility metrics
* ✅ Historical snapshot collection

### Currently being developed

* 🚧 Kafka event streaming
* 🚧 Real-time streaming consumers
* 🚧 Spark Structured Streaming
* 🚧 Automated orchestration
* 🚧 Static GTFS reference data integration
* 🚧 Data quality framework
* 🚧 Real-time dashboard
* 🚧 Mobility anomaly detection

---

# 📊 Current Data Source

The current MVP uses **Delhi Open Transit Data (OTD)** for real-time public transportation information.

The real-time feed provides GTFS-Realtime vehicle information such as:

* Vehicle ID
* Route ID
* Trip ID
* Latitude
* Longitude
* Vehicle timestamp
* Vehicle status

Official source:

[Delhi Open Transit Data](https://otd.delhi.gov.in/)

---

# 🏗️ Current Architecture

The project currently follows a Medallion-style architecture.

```text
                 Delhi OTD
                    │
                    ▼
             Python Ingestion
                    │
                    ▼
               🥉 BRONZE
            Raw GTFS-RT Files
                    │
                    ▼
             Data Transformation
                    │
                    ▼
               🥈 SILVER
        Clean Vehicle Position Data
                    │
                    ▼
              🥇 GOLD
          Mobility Analytics
                    │
             ┌──────┴──────┐
             ▼             ▼
       Route Metrics    Snapshot Metrics
```

The architecture will evolve as streaming components are introduced.

---

# 📁 Project Structure

```text
open-city-mobility/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── reference/
│
├── src/
│   ├── ingestion/
│   │   └── delhi_transit.py
│   │
│   └── transformation/
│       ├── vehicle_positions.py
│       ├── build_historical_silver.py
│       ├── hourly_vehicle_activity.py
│       ├── vehicle_movement.py
│       ├── route_performance.py
│       └── snapshot_metrics.py
│
├── docs/
│
├── README.md
├── LICENSE
└── .gitignore
```

---

# 🥉 Bronze Layer

The Bronze layer stores the raw GTFS-Realtime responses exactly as received.

Example:

```text
data/bronze/
├── vehicle_positions_20260912_100044.bin
├── vehicle_positions_20260912_100052.bin
├── vehicle_positions_20260912_105335.bin
└── ...
```

### Why keep raw data?

Because raw data gives us the ability to:

* Reprocess historical data
* Fix transformation logic
* Debug ingestion problems
* Rebuild downstream tables
* Investigate data-quality issues

The Bronze layer should remain as close as possible to the original source.

---

# 🥈 Silver Layer

The Silver layer contains decoded and cleaned vehicle-position data.

Current schema:

| Column                | Description                           |
| --------------------- | ------------------------------------- |
| `vehicle_id`          | Unique vehicle identifier             |
| `route_id`            | Transit route identifier              |
| `trip_id`             | Trip identifier                       |
| `latitude`            | Vehicle latitude                      |
| `longitude`           | Vehicle longitude                     |
| `vehicle_timestamp`   | Time reported by the vehicle feed     |
| `ingestion_timestamp` | Time the pipeline ingested the record |
| `vehicle_status`      | GTFS-Realtime vehicle status          |

Historical data is stored in:

```text
data/silver/vehicle_positions_history.parquet
```

---

# 🥇 Gold Layer

The current Gold layer contains analytical datasets.

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

* Distance travelled between observations
* Observation time difference
* Estimated speed
* GPS speed anomaly indicators

### Route Performance

```text
route_performance.parquet
```

Provides:

* Vehicle count
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

Provides one row per realtime feed snapshot:

* Snapshot timestamp
* Ingestion timestamp
* Active vehicles
* Active routes
* Total records

---

# 🔄 Current Data Flow

The current MVP works like this:

```text
1. Call Delhi OTD API
          ↓
2. Receive GTFS-Realtime binary feed
          ↓
3. Save raw response
          ↓
4. Decode protobuf data
          ↓
5. Clean and validate records
          ↓
6. Store Silver Parquet data
          ↓
7. Build analytical Gold tables
```

---

# 📈 Example Current Dataset

During development, the realtime feed produced thousands of vehicle records per snapshot.

Example development snapshot:

```text
Active vehicles: 5,723
Active routes:   1,355
Total records:   5,723
```

Historical development data has already been collected across multiple snapshots and is being used to build time-based mobility analytics.

> These numbers change continuously because the source is realtime.

---

# 🧠 Data Engineering Concepts Demonstrated

This project is intentionally designed to demonstrate real Data Engineering concepts rather than only dashboard creation.

### Data ingestion

```text
REST / Realtime API
```

### Data formats

```text
GTFS-Realtime
Protocol Buffers
Parquet
```

### Data processing

```text
Python
Pandas
```

### Data architecture

```text
Bronze
Silver
Gold
```

### Data quality

```text
Duplicate detection
Null handling
Coordinate validation
Timestamp validation
GPS anomaly detection
Observation-gap filtering
```

### Analytics

```text
Time-series analysis
Vehicle movement
Speed estimation
Route-level metrics
Snapshot-level metrics
```

---

# 🛠️ Technology Roadmap

The project will gradually introduce the following technologies.

| Technology                   | Planned Usage               | Status |
| ---------------------------- | --------------------------- | ------ |
| Python                       | Ingestion & transformations | ✅      |
| Pandas                       | Data processing             | ✅      |
| Parquet                      | Data storage                | ✅      |
| PostgreSQL                   | Analytics serving layer     | 🔜     |
| Kafka                        | Event streaming             | 🔜     |
| Spark                        | Stream processing           | 🔜     |
| Airflow                      | Orchestration               | 🔜     |
| Docker                       | Local development           | 🔜     |
| dbt                          | Transformation / modeling   | 🔜     |
| Great Expectations / similar | Data quality                | 🔜     |
| Grafana / Superset           | Dashboarding                | 🔜     |
| Terraform                    | Infrastructure              | 🔜     |
| Cloud deployment             | Production deployment       | 🔜     |
| ML                           | Mobility prediction         | 🔜     |

---

# 🌍 Future Data Sources

The long-term platform will support multiple mobility-related datasets.

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

The project is being designed so that contributors can add new cities without rewriting the entire pipeline.

Long-term structure:

```text
cities/
├── delhi/
├── mumbai/
├── bangalore/
├── london/
└── new_york/
```

The goal is:

```text
            Open City Mobility
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
     Delhi        Mumbai      Bangalore
       │            │            │
       └────────────┼────────────┘
                    ▼
              Common Pipeline
```

---

# 🤝 Contributing

This project is intended to be community-driven.

Contributors can work on different parts of the platform without needing to understand the entire codebase.

### Good first contributions

* Improve documentation
* Add tests
* Add city configuration
* Add a new API connector
* Improve error handling
* Improve data validation

### Data Engineering contributions

* Build Silver transformations
* Build Gold models
* Add Kafka producers/consumers
* Add Spark streaming jobs
* Improve partitioning
* Improve processing efficiency
* Add data-quality checks

### Advanced contributions

* Real-time anomaly detection
* Pipeline observability
* ML-based mobility prediction
* Kubernetes deployment
* Cloud infrastructure
* Data lineage
* Performance optimization

---

# 🧪 Development Principles

The project follows a few important principles.

### Raw data should remain reproducible

Bronze data should be retained so downstream datasets can be rebuilt.

### Transformations should be modular

Each transformation should have a clear input and output.

### Data quality should happen before analytics

Bad data should not silently become business insights.

### Small changes should be easy to contribute

Contributors should be able to work on isolated components.

### Production thinking

The project should gradually move toward:

```text
Scalable
Testable
Observable
Recoverable
Documented
Reproducible
```

---

# 🗺️ Roadmap

## Phase 1 — MVP

* [x] Delhi realtime ingestion
* [x] Bronze storage
* [x] Silver transformation
* [x] Historical data processing
* [x] Initial Gold analytics

## Phase 2 — Streaming

* [ ] Kafka producer
* [ ] Kafka topic design
* [ ] Kafka consumer
* [ ] Event schema
* [ ] Message validation

## Phase 3 — Stream Processing

* [ ] Spark Structured Streaming
* [ ] Streaming Bronze
* [ ] Streaming Silver
* [ ] Real-time aggregations

## Phase 4 — Orchestration

* [ ] Airflow
* [ ] Scheduled ingestion
* [ ] Pipeline dependencies
* [ ] Retry handling
* [ ] Failure notifications

## Phase 5 — Analytics Platform

* [ ] PostgreSQL serving layer
* [ ] Route dimension
* [ ] Stop dimension
* [ ] Better route performance models
* [ ] Real-time dashboard

## Phase 6 — Open Source Expansion

* [ ] Contributor guide
* [ ] Issue templates
* [ ] Pull request templates
* [ ] Automated tests
* [ ] CI/CD
* [ ] Multi-city support

## Phase 7 — Advanced Platform

* [ ] Data observability
* [ ] Anomaly detection
* [ ] Mobility prediction
* [ ] Cloud deployment
* [ ] Infrastructure as Code
* [ ] Kubernetes

---

# 📊 Dashboard

The planned dashboard will provide:

* Live vehicle map
* Active vehicle count
* Active route count
* Route performance
* Vehicle movement
* Mobility trends
* Anomaly alerts
* Historical comparisons

### Dashboard Preview

> Add the dashboard screenshot here once the dashboard is available.

```text
![Dashboard](docs/images/dashboard.png)
```

---

# 🔐 Security

Never commit API keys or credentials.

Use environment variables:

```text
DELHI_TRANSIT_API_KEY=your_api_key
```

The `.env` file must remain outside version control.

---

# 📄 License

This project is open source.

See [`LICENSE`](LICENSE) for license information.

---

# ⭐ Contributing & Supporting

If you find this project useful:

* ⭐ Star the repository
* 🐛 Report bugs
* 💡 Suggest improvements
* 🔧 Submit pull requests
* 📖 Improve documentation

Every contribution helps make the platform better.

---

## 🚦 Project Goal

The ultimate goal is to transform this:

```text
Raw public transportation feeds
```

into this:

```text
A reusable open-source real-time
city mobility intelligence platform.
```

Built by the community. For the community.
