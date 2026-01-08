# Microsoft Fabric Data Engineering Project
# Data Dictionary

## Overview

This document describes all data assets in the Fabric Analytics Platform.

---

## Bronze Layer (Raw Data)

### raw_taxi_trips
**Source:** NYC TLC (Taxi and Limousine Commission)  
**Format:** Parquet  
**Location:** Bronze Lakehouse → Files/raw_taxi_trips.parquet  
**Records:** ~4 million (October 2025)

| Column | Type | Description |
|--------|------|-------------|
| VendorID | int | TPEP provider (1=CMT, 2=VeriFone) |
| tpep_pickup_datetime | timestamp | Pickup date and time |
| tpep_dropoff_datetime | timestamp | Dropoff date and time |
| passenger_count | int | Number of passengers |
| trip_distance | float | Trip distance in miles |
| PULocationID | int | Pickup location zone ID |
| DOLocationID | int | Dropoff location zone ID |
| payment_type | int | Payment method (1=Credit, 2=Cash, etc.) |
| fare_amount | float | Base fare in USD |
| tip_amount | float | Tip amount in USD |
| total_amount | float | Total paid in USD |

---

### raw_gdp
**Source:** World Bank API  
**Format:** JSON → Delta Table  
**Location:** Bronze Lakehouse → Tables/dbo/raw_gdp  
**Records:** 65 (yearly data 1960-2024)

| Column | Type | Description |
|--------|------|-------------|
| countryiso3code | string | Country ISO code (USA) |
| date | string | Year |
| value | float | GDP in current USD |
| indicator_id | string | Indicator code |
| indicator_value | string | Indicator name |

---

### raw_fx_rates
**Source:** ECB Data API  
**Format:** CSV → Delta Table  
**Location:** Bronze Lakehouse → Tables/dbo/raw_fx_rates  
**Records:** ~7,000 (daily rates)

| Column | Type | Description |
|--------|------|-------------|
| TIME_PERIOD | date | Date of rate |
| OBS_VALUE | float | USD/EUR exchange rate |
| FREQ | string | Frequency (D=Daily) |
| CURRENCY | string | Currency pair |

---

### raw_open_aq
**Source:** OpenAQ API  
**Format:** JSON → Delta Table  
**Location:** Bronze Lakehouse → Tables/dbo/raw_open_aq  
**Records:** ~50-100 locations (NYC area)

| Column | Type | Description |
|--------|------|-------------|
| id | int | Location ID |
| name | string | Location name |
| latitude | float | GPS latitude |
| longitude | float | GPS longitude |
| sensors | array | Sensor data with parameters |

---

## Silver Layer (Cleaned Data)

### cleaned_taxi_trips
**Records:** ~4 million

| Column | Type | Description |
|--------|------|-------------|
| pickup_datetime | timestamp | Cleaned pickup time |
| dropoff_datetime | timestamp | Cleaned dropoff time |
| passenger_count | int | Passengers (validated) |
| trip_distance | float | Distance in miles |
| pickup_location_id | int | Pickup zone |
| dropoff_location_id | int | Dropoff zone |
| fare_amount | float | Fare USD |
| tip_amount | float | Tip USD |
| total_amount | float | Total USD |
| payment_type | int | Payment method |

---

### cleaned_gdp
**Records:** 65

| Column | Type | Description |
|--------|------|-------------|
| year | int | Year |
| country_code | string | Country (USA) |
| gdp_usd | float | GDP in USD |

---

### cleaned_fx_rates
**Records:** ~7,000

| Column | Type | Description |
|--------|------|-------------|
| date | date | Rate date |
| usd_eur_rate | float | USD to EUR rate |

---

## Gold Layer (Star Schema)

### Dimension Tables

#### DimDate
**Records:** 731 (2025-2026)

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | Surrogate key (YYYYMMDD) |
| full_date | date | Full date |
| year | int | Year |
| quarter | int | Quarter (1-4) |
| month | int | Month (1-12) |
| month_name | string | Month name |
| week | int | Week of year |
| day_of_week | int | Day of week (1-7) |
| day_name | string | Day name |
| is_weekend | boolean | True if Saturday or Sunday |

#### DimFX
**Records:** ~7,000

| Column | Type | Description |
|--------|------|-------------|
| fx_key | int | Surrogate key |
| fx_date | date | Rate date |
| usd_eur_rate | decimal(10,6) | Exchange rate |

#### DimGDP
**Records:** 65

| Column | Type | Description |
|--------|------|-------------|
| gdp_key | int | Surrogate key |
| year | int | Year |
| country_code | string | Country |
| gdp_usd | decimal(20,2) | GDP value |

#### DimZone
**Records:** ~50 (OpenAQ locations mapped to NYC Taxi Zones)

| Column | Type | Description |
|--------|------|-------------|
| zone_key | int | Surrogate key |
| zone_id | int | Location ID |
| zone_name | string | Zone name |
| borough | string | NYC Borough |
| taxi_zone_id | int | Nearest taxi zone ID |
| taxi_zone_name | string | Taxi zone name |
| latitude | double | GPS latitude |
| longitude | double | GPS longitude |

---

### Fact Tables

#### FactTaxiDaily
**Records:** ~200,000 (aggregated by date and zone)

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | FK to DimDate |
| pickup_zone_id | int | Pickup zone |
| dropoff_zone_id | int | Dropoff zone |
| total_trips | int | Count of trips |
| total_passengers | int | Sum of passengers |
| total_distance | float | Sum of distance |
| total_fare | float | Sum of fares |
| total_tips | float | Sum of tips |
| avg_fare | float | Average fare |
| avg_trip_distance | float | Average distance |

#### FactAirQualityDaily
**Records:** Variable (daily measurements)

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | FK to DimDate |
| location_id | int | Measurement location ID |
| taxi_zone_id | int | Mapped taxi zone ID |
| pm25 | decimal(5,1) | PM2.5 concentration (µg/m³) |
| no2 | decimal(5,1) | NO2 concentration (ppb) |
| o3 | decimal(5,1) | Ozone concentration (ppb) |
| aqi | int | Air Quality Index |
