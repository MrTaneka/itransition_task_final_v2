# Integration Tasks Architecture

This document describes the architecture of additional integration tasks
implemented for the Itransition Data Engineering project.

---

## Overview

Three integration tasks extend the core Microsoft Fabric pipeline:

| Task | Purpose | Technology |
|------|---------|------------|
| **Weather + Grafana** | Time-series visualization | InfluxDB, Grafana, Open-Meteo API |
| **Great Expectations** | Data quality validation | Telegram Bot, Custom GX Framework |
| **Power Automate** | Notifications | OneDrive, Gmail, Power Automate |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     PRESENTATION LAYER                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Grafana   │  │  Telegram   │  │  Power Automate Flow    │  │
│  │  Dashboard  │  │     Bot     │  │  (Email + Push)         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                      SERVICE LAYER                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Weather    │  │ Validation  │  │     Export              │  │
│  │  Ingestion  │  │  Service    │  │     Service             │  │
│  │  Service    │  │             │  │                         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                    DATA ACCESS LAYER                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  InfluxDB   │  │   Data      │  │  OneDrive + Local       │  │
│  │ Repository  │  │  Loader     │  │  Repositories           │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INFRASTRUCTURE                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  InfluxDB   │  │  Parquet    │  │   OneDrive Sync         │  │
│  │  (Docker)   │  │  (Gold)     │  │   Folder                │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Design Patterns

| Pattern | Implementation | Purpose |
|---------|---------------|---------|
| **Singleton** | `AppConfig` | Single configuration instance |
| **Factory** | `LoggerFactory` | Consistent logger creation |
| **Decorator** | `@with_retry` | Retry logic for API calls |
| **Repository** | `InfluxDBRepository`, `OneDriveRepository` | Data access abstraction |
| **Service** | All `*Service` classes | Business logic orchestration |
| **Strategy** | `Expectation` classes | Interchangeable validation rules |
| **Observer** | `ValidationNotifier` classes | Event-based notifications |
| **Builder** | `ReportBuilder` | Step-by-step report construction |
| **Dependency Injection** | Constructor injection | Testability and flexibility |

---

## Data Flow

### Weather Pipeline
```
Open-Meteo API → WeatherIngestionService → InfluxDBRepository → Grafana
```

### Data Quality Pipeline
```
Gold Layer Parquet → ValidationService → Expectations → TelegramNotifier
                                      ↓
                              Telegram Bot ← User /check command
```

### Export Pipeline
```
Gold Layer Parquet → ExportService → ReportBuilder → JSON
                                                    ↓
                                   OneDriveRepository → Power Automate
                                                              ↓
                                                    Email + Push Notification
```

---

## Configuration

All services configured via environment variables (`.env` file):

- `INFLUXDB_*` - InfluxDB connection
- `TELEGRAM_*` - Telegram bot settings
- `DATA_*` - File paths
- `ONEDRIVE_*` - OneDrive sync path

---

## Docker Services

```yaml
services:
  influxdb:
    image: influxdb:2.7
    ports: 8086:8086

  grafana:
    image: grafana/grafana:10.2.0
    ports: 3000:3000
```

---

## Related Files

- [additional_tasks/README.md](../additional_tasks/README.md) - Quick start guide
- [additional_tasks/core/__init__.py](../additional_tasks/core/__init__.py) - Shared infrastructure
- [additional_tasks/docker-compose.yml](../additional_tasks/docker-compose.yml) - Container setup
