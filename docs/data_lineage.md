# Data Lineage

## End-to-End Data Flow

```text
Delhi OTD API
      │
      ▼
Kafka - vehicle_positions
      │
      ▼
Bronze Layer
      │
      ▼
Silver Layer
      │
      ▼
Gold Transformations
      │
      ├── hourly_vehicle_activity
      ├── vehicle_movement
      ├── route_performance
      └── snapshot_metrics
      │
      ▼
PostgreSQL
      │
      ├── route_activity_summary
      └── vehicle_positions
      │
      ▼
FastAPI
      │
      ▼
Dashboard

Airflow
   │
   ├── Refresh Route Summary
   ├── Validate Route Summary
   ├── Validate Route IDs
   ├── Check Data Freshness
   └── Pipeline Monitoring