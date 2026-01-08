# Microsoft Fabric Data Engineering Project

## Overview

End-to-end data engineering solution using Microsoft Fabric:
- **Data Ingestion** (Bronze Layer)
- **Data Transformation** (Silver Layer)
- **Data Modeling** (Gold Layer - Star Schema)
- **Analytics & Visualization** (Power BI / Notebook)

---

## Project Structure

```
itransition_task_final/
├── Microsoft Fabric Data Engineering Project.docx  # Requirements
├── README.md                                        # This file
├── documentation/                                   # Documentation
│   ├── architecture.md                              # System architecture
│   ├── data_dictionary.md                           # Data schemas
│   └── lineage.md                                   # Data lineage
└── notebooks/                                       # PySpark notebooks
    ├── Complete_Pipeline_Final.py                   # Main ETL pipeline
    ├── Analytics_Visualizations.py                  # Q1-Q4 visualizations
    └── Data_Validation.py                           # Data quality checks
```

---

## Data Sources

| # | Source | Format | Fabric Component |
|---|--------|--------|------------------|
| 1 | NYC Taxi | Parquet | Pipeline_Ingestion |
| 2 | OpenAQ | JSON API | Complete_Pipeline_Final.py |
| 3 | World Bank GDP | JSON API | Dataflow_WorldBank_GDP |
| 4 | ECB FX Rates | CSV API | Dataflow_ECB_FX |

---

## Architecture (Medallion)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────────┐
│   Sources   │ -> │   Bronze    │ -> │   Silver    │ -> │     Gold     │
│  (APIs/     │    │ (Raw Data)  │    │ (Cleaned)   │    │ (Star Schema)│
│   Files)    │    │             │    │             │    │              │
└─────────────┘    └─────────────┘    └─────────────┘    └──────────────┘
                                                                │
                                                                ▼
                                                        ┌──────────────┐
                                                        │   Power BI   │
                                                        │  Dashboard   │
                                                        └──────────────┘
```

---

## Gold Layer (Star Schema)

### Dimensions:
- **DimDate** - Calendar 2025-2026 with is_weekend flag
- **DimZone** - OpenAQ locations mapped to taxi zones
- **DimFX** - USD/EUR exchange rates
- **DimGDP** - USA GDP by year

### Facts:
- **FactTaxiDaily** - Aggregated taxi metrics by zone and date
- **FactAirQualityDaily** - Air quality (PM2.5, NO2, O3, AQI)

---

## Business Questions (Q1-Q4)

| # | Question | Answer |
|---|----------|--------|
| Q1 | Traffic vs Air Quality | Weekday traffic higher, PM2.5 stable |
| Q2 | Zone Analysis | LaGuardia Airport shows taxi-pollution link |
| Q3 | Revenue USD vs EUR | Avg fare: ~$30 USD / ~€26 EUR |
| Q4 | Economic & Environmental | 4M trips, PM2.5 ~6 μg/m³ (Good) |

---

## Fabric Workspace Components

| Component | Type | Description |
|-----------|------|-------------|
| Bronze | Lakehouse | Raw data from sources |
| Silver | Lakehouse | Cleaned and normalized data |
| Gold | Lakehouse | Star Schema for analytics |
| GoldWarehouse | Warehouse | SQL endpoint for reporting |
| Pipeline_Ingestion | Pipeline | ETL for NYC Taxi |
| Dataflow_Open_AQ | Dataflow Gen2 | OpenAQ API ingestion |
| Dataflow_ECB_FX | Dataflow Gen2 | ECB FX rates ingestion |
| Dataflow_WorldBank_GDP | Dataflow Gen2 | World Bank GDP ingestion |
| Power BI Dashboard | Report | Q1-Q4 visualizations |

---

## Documentation

- [Architecture](documentation/architecture.md)
- [Data Dictionary](documentation/data_dictionary.md)
- [Data Lineage](documentation/lineage.md)

---

*Project completed: January 2026*
