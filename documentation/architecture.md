# Microsoft Fabric Data Engineering Project
# Architecture Documentation

## Solution Overview

This document describes the architecture of the unified analytics platform built on Microsoft Fabric.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                   │
├─────────────┬─────────────┬─────────────┬───────────────────────────────────┤
│  NYC Taxi   │   World     │    ECB      │            OpenAQ                 │
│  Parquet    │  Bank API   │   CSV API   │           JSON API                │
│  (~47MB)    │   (JSON)    │   (CSV)     │                                   │
└──────┬──────┴──────┬──────┴──────┬──────┴──────────────┬────────────────────┘
       │             │             │                     │
       ▼             ▼             ▼                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MICROSOFT FABRIC                                    │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                     BRONZE LAYER (Raw Data)                            │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │ │
│  │  │raw_taxi_trips│ │   raw_gdp    │ │ raw_fx_rates │ │ raw_open_aq  │   │ │
│  │  │   (~4M)      │ │    (65)      │ │   (~7,000)   │ │   (~100)     │   │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼ PySpark Notebook                       │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                     SILVER LAYER (Cleaned)                             │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                    │ │
│  │  │cleaned_taxi  │ │ cleaned_gdp  │ │ cleaned_fx   │                    │ │
│  │  │   (~4M)      │ │    (65)      │ │   (~7,000)   │                    │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼ PySpark Aggregation                    │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                     GOLD LAYER (Star Schema)                           │ │
│  │                                                                        │ │
│  │  DIMENSIONS:                     FACTS:                                │ │
│  │  ┌──────────┐ ┌──────────┐      ┌──────────────────┐                   │ │
│  │  │ DimDate  │ │  DimFX   │      │  FactTaxiDaily   │                   │ │
│  │  │  (731)   │ │ (~7,000) │      │    (~200K)       │                   │ │
│  │  └──────────┘ └──────────┘      └──────────────────┘                   │ │
│  │  ┌──────────┐ ┌──────────┐      ┌──────────────────┐                   │ │
│  │  │ DimGDP   │ │ DimZone  │      │FactAirQualityDaily│                  │ │
│  │  │   (65)   │ │  (~50)   │      │    (~100)        │                   │ │
│  │  └──────────┘ └──────────┘      └──────────────────┘                   │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                        │
│                                    ▼                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       POWER BI DASHBOARD                               │ │
│  │  ┌──────────┐ ┌──────────────┐ ┌──────────────────┐                    │ │
│  │  │   Card   │ │  Bar Chart   │ │    Line Chart    │                    │ │
│  │  │ Metrics  │ │ Fare by Zone │ │  Trips/PM2.5     │                    │ │
│  │  └──────────┘ └──────────────┘ └──────────────────┘                    │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Components

### Microsoft Fabric Components

| Component | Name | Purpose |
|-----------|------|---------|
| Workspace | FabricDataEngineering | Main workspace |
| Lakehouse | Bronze | Raw data storage |
| Lakehouse | Silver | Cleaned data storage |
| Lakehouse | Gold | Star schema storage |
| Warehouse | GoldWarehouse | SQL analytics endpoint |
| Notebook | Complete_Pipeline_Final | ETL transformations |
| Data Factory | Pipelines | Data ingestion |
| Data Factory | Dataflows Gen2 | API ingestion |
| Power BI | Dashboard | Visualizations |

---

## Data Flow

### Ingestion Methods

1. **NYC Taxi (Parquet)**
   - Method: Data Pipeline → Copy Activity
   - Source: TLC CloudFront URL
   - Destination: Bronze/Files

2. **World Bank GDP (JSON API)**
   - Method: Dataflow Gen2
   - Transform: JSON → Table
   - Destination: Bronze/Tables

3. **ECB FX Rates (CSV API)**
   - Method: Dataflow Gen2
   - Transform: CSV → Table
   - Destination: Bronze/Tables

4. **OpenAQ Air Quality (JSON API)**
   - Method: PySpark Notebook (API call)
   - Transform: JSON → Delta Table
   - Destination: Gold/Tables

### Transformation Pipeline

```
Bronze → [PySpark Notebook] → Silver → [PySpark Aggregation] → Gold
```

**Transformations Applied:**
- NULL value filtering
- Data type casting
- Column renaming
- Date extraction
- Aggregation by date/zone
- Star schema modeling

---

## Star Schema Design

```
                    ┌─────────────┐
                    │   DimDate   │
                    │─────────────│
                    │ date_key PK │
                    │ full_date   │
                    │ year        │
                    │ quarter     │
                    │ month       │
                    │ week        │
                    │ day_of_week │
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  FactTaxiDaily  │ │FactAirQuality  │ │     DimFX       │
│─────────────────│ │  Daily         │ │─────────────────│
│ date_key FK     │ │─────────────────│ │ fx_key PK       │
│ pickup_zone_id  │ │ date_key FK     │ │ fx_date         │
│ dropoff_zone_id │ │ location_id     │ │ usd_eur_rate    │
│ total_trips     │ │ taxi_zone_id    │ └─────────────────┘
│ total_passengers│ │ pm25            │
│ total_distance  │ │ no2             │ ┌─────────────────┐
│ total_fare      │ │ o3              │ │    DimGDP       │
│ total_tips      │ │ aqi             │ │─────────────────│
│ avg_fare        │ └─────────────────┘ │ gdp_key PK      │
└─────────────────┘                     │ year            │
                                        │ country_code    │
┌─────────────────┐                     │ gdp_usd         │
│    DimZone      │                     └─────────────────┘
│─────────────────│
│ zone_key PK     │
│ zone_id         │
│ zone_name       │
│ borough         │
│ taxi_zone_id    │
│ taxi_zone_name  │
└─────────────────┘
```

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Cloud Platform | Microsoft Fabric |
| Data Lake | OneLake |
| Data Format | Delta Lake (Parquet) |
| ETL | PySpark, Dataflows Gen2 |
| SQL Engine | Fabric Warehouse |
| Visualization | Power BI |
| Orchestration | Data Factory Pipelines |

---

## Security & Governance

- **Authentication:** Microsoft Entra ID
- **Authorization:** Workspace roles (Admin, Member, Contributor)
- **Data Lineage:** Tracked via Fabric metadata
- **Encryption:** Data encrypted at rest and in transit
- **Audit:** Activity logs in Fabric Admin portal
