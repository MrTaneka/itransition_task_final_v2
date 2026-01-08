# Microsoft Fabric Data Engineering Project
# Data Lineage Documentation

## Overview

This document traces data flow from source to destination across all layers.

---

## NYC Taxi Data Lineage

```
Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-10.parquet
    │
    ▼ [Data Pipeline: Copy Activity]
    │
Bronze: raw_taxi_trips.parquet (~4M rows)
    │   Columns: VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
    │           passenger_count, trip_distance, PULocationID, DOLocationID,
    │           payment_type, fare_amount, tip_amount, total_amount
    │
    ▼ [PySpark Notebook: Complete_Pipeline_Final.py]
    │   - Filter: trip_distance > 0, fare_amount > 0
    │   - Rename columns to snake_case
    │   - Cast data types
    │
Silver: cleaned_taxi_trips (~4M rows)
    │   Columns: pickup_datetime, dropoff_datetime, passenger_count,
    │           trip_distance, pickup_location_id, dropoff_location_id,
    │           fare_amount, tip_amount, total_amount, payment_type
    │
    ▼ [PySpark Notebook: Aggregation]
    │   - Group by date_key, pickup_zone_id, dropoff_zone_id
    │   - Calculate: SUM(trips), SUM(passengers), SUM(distance),
    │               SUM(fare), SUM(tips), AVG(fare), AVG(distance)
    │
Gold: FactTaxiDaily (~200K rows)
        Columns: date_key, pickup_zone_id, dropoff_zone_id, total_trips,
                total_passengers, total_distance, total_fare, total_tips,
                avg_fare, avg_trip_distance
```

---

## World Bank GDP Data Lineage

```
Source: https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json
    │
    ▼ [Dataflow Gen2: Dataflow_WorldBank_GDP]
    │   - Parse JSON array
    │   - Extract year, value, country code
    │   - Cast types
    │
Bronze: raw_gdp (65 rows)
    │   Columns: countryiso3code, date, value, indicator_id, indicator_value
    │
    ▼ [PySpark Notebook]
    │   - Filter non-null values
    │   - Cast year to integer
    │   - Cast GDP to float
    │   - Rename columns
    │
Silver: cleaned_gdp (65 rows)
    │   Columns: year, country_code, gdp_usd
    │
    ▼ [PySpark Notebook]
    │   - Add surrogate key (gdp_key)
    │
Gold: DimGDP (65 rows)
        Columns: gdp_key, year, country_code, gdp_usd
```

---

## ECB FX Rates Data Lineage

```
Source: https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata
    │
    ▼ [Dataflow Gen2: Dataflow_ECB_FX]
    │   - Parse CSV
    │   - Skip metadata rows
    │   - Extract TIME_PERIOD, OBS_VALUE
    │
Bronze: raw_fx_rates (~7,000 rows)
    │   Columns: TIME_PERIOD, OBS_VALUE, FREQ, CURRENCY, etc.
    │
    ▼ [PySpark Notebook]
    │   - Filter non-null rates
    │   - Cast date and rate types
    │   - Rename columns
    │
Silver: cleaned_fx_rates (~7,000 rows)
    │   Columns: date, usd_eur_rate
    │
    ▼ [PySpark Notebook]
    │   - Add surrogate key (fx_key)
    │
Gold: DimFX (~7,000 rows)
        Columns: fx_key, fx_date, usd_eur_rate
```

---

## OpenAQ Air Quality Data Lineage

```
Source: https://api.openaq.org/v3/locations (NYC area, 25km radius)
    │
    ▼ [PySpark Notebook: Complete_Pipeline_Final.py]
    │   - API call with pagination
    │   - Extract locations with sensors
    │   - Map to nearest NYC taxi zones (Haversine distance)
    │
Gold: DimZone (~50 rows)
    │   Columns: zone_key, zone_id, zone_name, borough, 
    │           taxi_zone_id, taxi_zone_name, latitude, longitude
    │
    ▼ [PySpark Notebook: Fetch measurements]
    │   - Get daily measurements for PM2.5, NO2, O3
    │   - Calculate AQI from PM2.5
    │   - Pivot parameters to columns
    │
Gold: FactAirQualityDaily (variable rows)
        Columns: date_key, location_id, taxi_zone_id, pm25, no2, o3, aqi
```

---

## DimDate Generation

```
Generated programmatically (not from source data)
    │
    ▼ [PySpark Notebook]
    │   - Generate dates for years 2025-2026 (731 days)
    │   - Calculate: year, quarter, month, month_name, week,
    │               day_of_week, day_name
    │   - Create date_key as YYYYMMDD integer
    │
Gold: DimDate (731 rows)
        Columns: date_key, full_date, year, quarter, month,
                month_name, week, day_of_week, day_name
```

---

## Summary Table

| Source | Bronze | Silver | Gold | Records |
|--------|--------|--------|------|---------|
| NYC TLC | raw_taxi_trips | cleaned_taxi_trips | FactTaxiDaily | ~4M → ~4M → ~200K |
| World Bank | raw_gdp | cleaned_gdp | DimGDP | 65 → 65 → 65 |
| ECB | raw_fx_rates | cleaned_fx_rates | DimFX | ~7K → ~7K → ~7K |
| OpenAQ | (API) | (API) | DimZone + FactAirQualityDaily | ~50 locations |
| Generated | - | - | DimDate | 731 |
