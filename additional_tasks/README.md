# Integration Tasks - Enterprise Architecture

Additional integration tasks for Microsoft Fabric data engineering project.
Demonstrates integration with Grafana, Great Expectations, and Power Automate.

---

## Architecture Overview

```
additional_tasks/
├── core/                    # Shared infrastructure (Singleton, Factory, Decorator)
│   └── __init__.py         # Config, Logger, Retry, Error handling
├── weather/                 # Phase 1: Grafana + Weather
│   └── service.py          # WeatherIngestionService (Repository pattern)
├── great_expectations/      # Phase 2: Data Quality
│   └── service.py          # ValidationService (Strategy, Observer patterns)
├── power_automate/          # Phase 3: JSON Export
│   └── service.py          # ExportService (Builder, Repository patterns)
├── grafana/                 # Grafana provisioning
├── docker-compose.yml       # InfluxDB + Grafana containers
├── .env                     # Environment configuration
└── README.md               # This file
```

---

## Design Patterns Used

| Pattern | Location | Purpose |
|---------|----------|---------|
| **Singleton** | `core/AppConfig` | Single configuration instance |
| **Factory** | `core/LoggerFactory` | Logger creation |
| **Decorator** | `core/with_retry` | Retry logic for API calls |
| **Repository** | All services | Data persistence abstraction |
| **Service** | All services | Business logic orchestration |
| **Strategy** | `great_expectations` | Interchangeable expectations |
| **Observer** | `great_expectations` | Notification subscribers |
| **Builder** | `power_automate` | Report construction |
| **Dependency Injection** | All services | Testability and flexibility |

---

## Quick Start

### 1. Install Dependencies
```bash
pip install python-dotenv influxdb-client requests aiohttp pandas numpy pyarrow
```

### 2. Start Infrastructure
```bash
docker-compose up -d
```

### 3. Run Weather Ingestion
```bash
python -m weather.service
```

### 4. Run Data Quality Validation
```bash
python -m great_expectations.service
# Or start Telegram bot:
python -m great_expectations.service --bot
```

### 5. Run Power Automate Export
```bash
python -m power_automate.service
```

---

## Configuration

All configuration via `.env` file:

```env
# InfluxDB
INFLUXDB_URL=http://localhost:8086
INFLUXDB_ORG=fabric_org
INFLUXDB_BUCKET=weather_data
INFLUXDB_TOKEN=fabric-weather-token-2024

# Telegram
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_BOT_NAME=ItransitionProjectBot

# Data Paths
DATA_GOLD_LAYER_PATH=d:/itransition_task_final/data/gold/FactTaxiDaily.parquet
ONEDRIVE_SYNC_PATH=C:/Users/Taneka/OneDrive/FabricReports
```

---

## Service Details

### Weather Service
- Fetches weather data from Open-Meteo API
- Stores in InfluxDB time-series database
- Supports daemon mode for periodic execution
- Grafana dashboard for visualization

### Data Quality Service
- Great Expectations-inspired validation framework
- 14+ built-in expectations (nulls, ranges, distributions)
- Telegram bot for user-friendly trigger
- Extensible via Strategy pattern

### Export Service
- Generates JSON reports for Power Automate
- Uploads to OneDrive sync folder
- Power Automate flow sends email + push notifications
- Builder pattern for flexible report structure

---

## URLs

| Service | URL |
|---------|-----|
| Grafana | http://localhost:3000 (admin/admin) |
| InfluxDB | http://localhost:8086 |
| Telegram Bot | @ItransitionProjectBot |

---

## Author

Senior Data Engineer  
Itransition Project 2025
