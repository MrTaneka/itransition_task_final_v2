# ==============================================================================
# COMPLETE DATA PIPELINE
# ==============================================================================
# Microsoft Fabric Data Engineering Pipeline
# Bronze -> Silver -> Gold layer transformations with Star Schema
#
# Version: 1.0
# ==============================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, year, month, dayofmonth, quarter, weekofyear,
    dayofweek, date_format, to_date, monotonically_increasing_id,
    count, sum, avg, round as spark_round, lit, current_date, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import date, timedelta, datetime
from typing import List, Dict
from math import radians, cos, sin, asin, sqrt
import requests
import logging
import os

# ==============================================================================
# CONFIGURATION
# ==============================================================================
class Config:
    """Configuration constants for data pipeline paths and API settings."""
    # OpenAQ API key for air quality data ingestion
    OPENAQ_API_KEY = "YOUR_API_KEY_HERE"
    WORKSPACE = "FabricDataEngineering"
    BRONZE_PATH = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse"
    SILVER_PATH = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/Silver.Lakehouse"
    GOLD_PATH = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/Gold.Lakehouse"
    
    DATE_KEY_YEAR_MULTIPLIER = 10000
    DATE_KEY_MONTH_MULTIPLIER = 100
    START_YEAR = 2025
    END_YEAR = 2026
    
    OPENAQ_BASE_URL = "https://api.openaq.org/v3"
    NYC_LAT = 40.7128
    NYC_LON = -74.0060


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_success(table_name: str, count: int) -> None:
    """Log successful table write with row count."""
    print(f"[SUCCESS] {table_name}: {count:,} rows")


# ==============================================================================
# I/O FUNCTIONS
# ==============================================================================
def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)

def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)

def write_delta(df: DataFrame, path: str) -> int:
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(path)
    return df.count()


# ==============================================================================
# BRONZE → SILVER: TAXI
# ==============================================================================
def read_taxi_bronze(spark: SparkSession) -> DataFrame:
    return read_parquet(spark, f"{Config.BRONZE_PATH}/Files/raw_taxi_trips.parquet")

def clean_taxi(df: DataFrame) -> DataFrame:
    return df.select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("passenger_count"),
        col("trip_distance"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("fare_amount"),
        col("tip_amount"),
        col("total_amount"),
        col("payment_type")
    ).filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

def save_taxi_silver(df: DataFrame) -> int:
    return write_delta(df, f"{Config.SILVER_PATH}/Tables/cleaned_taxi_trips")


# ==============================================================================
# BRONZE → SILVER: GDP
# ==============================================================================
def read_gdp_bronze(spark: SparkSession) -> DataFrame:
    return read_delta(spark, f"{Config.BRONZE_PATH}/Tables/dbo/raw_gdp")

def clean_gdp(df: DataFrame) -> DataFrame:
    return df.select(
        col("id").alias("country_code"),
        col("date").cast("int").alias("year"),
        col("value").cast("double").alias("gdp_usd")
    )

def save_gdp_silver(df: DataFrame) -> int:
    return write_delta(df, f"{Config.SILVER_PATH}/Tables/cleaned_gdp")


# ==============================================================================
# BRONZE → SILVER: FX RATES
# ==============================================================================
def read_fx_bronze(spark: SparkSession) -> DataFrame:
    return read_delta(spark, f"{Config.BRONZE_PATH}/Tables/dbo/raw_fx_rates")

def clean_fx(df: DataFrame) -> DataFrame:
    return df.select(
        col("TIME_PERIOD").alias("date"),
        col("OBS_VALUE").cast("double").alias("usd_eur_rate")
    ).filter(col("OBS_VALUE").isNotNull())

def save_fx_silver(df: DataFrame) -> int:
    return write_delta(df, f"{Config.SILVER_PATH}/Tables/cleaned_fx_rates")


# ==============================================================================
# SILVER → GOLD: DIMENSIONS
# ==============================================================================
def build_dim_date(spark: SparkSession) -> DataFrame:
    """Generate DimDate covering 2025-2026."""
    all_dates = []
    for yr in range(Config.START_YEAR, Config.END_YEAR + 1):
        days_in_year = 366 if yr % 4 == 0 else 365
        all_dates.extend([(date(yr, 1, 1) + timedelta(days=i),) for i in range(days_in_year)])
    
    date_df = spark.createDataFrame(all_dates, ["full_date"])
    
    return date_df.select(
        (year("full_date") * 10000 + month("full_date") * 100 + dayofmonth("full_date")).cast("int").alias("date_key"),
        col("full_date"),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month"),
        date_format("full_date", "MMMM").alias("month_name"),
        weekofyear("full_date").alias("week"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        # is_weekend: Saturday (7) or Sunday (1) in Spark
        when((dayofweek("full_date") == 1) | (dayofweek("full_date") == 7), True).otherwise(False).alias("is_weekend")
    )

def build_dim_fx(spark: SparkSession) -> DataFrame:
    fx = read_delta(spark, f"{Config.SILVER_PATH}/Tables/cleaned_fx_rates")
    return fx.select(
        monotonically_increasing_id().cast("int").alias("fx_key"),
        to_date(col("date")).alias("fx_date"),
        col("usd_eur_rate").cast("decimal(10,6)")
    ).filter(col("usd_eur_rate").isNotNull())

def build_dim_gdp(spark: SparkSession) -> DataFrame:
    gdp = read_delta(spark, f"{Config.SILVER_PATH}/Tables/cleaned_gdp")
    return gdp.select(
        monotonically_increasing_id().cast("int").alias("gdp_key"),
        col("year").cast("int"),
        col("country_code"),
        col("gdp_usd").cast("decimal(20,2)")
    ).filter(col("gdp_usd").isNotNull())


# ==============================================================================
# SILVER → GOLD: FACTS
# ==============================================================================
def build_fact_taxi_daily(spark: SparkSession) -> DataFrame:
    taxi = read_delta(spark, f"{Config.SILVER_PATH}/Tables/cleaned_taxi_trips")
    
    return taxi.groupBy(
        (year("pickup_datetime") * 10000 + month("pickup_datetime") * 100 + dayofmonth("pickup_datetime")).cast("int").alias("date_key"),
        col("pickup_location_id").alias("pickup_zone_id"),
        col("dropoff_location_id").alias("dropoff_zone_id")
    ).agg(
        count("*").alias("total_trips"),
        sum("passenger_count").cast("int").alias("total_passengers"),
        spark_round(sum("trip_distance"), 2).alias("total_distance"),
        spark_round(sum("fare_amount"), 2).alias("total_fare"),
        spark_round(sum("tip_amount"), 2).alias("total_tips"),
        spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare")
    )


# ==============================================================================
# TAXI ZONES LOOKUP (для маппинга OpenAQ станций)
# ==============================================================================
# Source: NYC TLC Taxi Zone Lookup Table
# URL: https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc
# Coordinates: Centroids calculated from official zone polygons
# Selection: Top 40 zones by taxi volume (Manhattan/Brooklyn/Queens)
# ==============================================================================
NYC_TAXI_ZONES = [
    {"zone_id": 4, "zone_name": "Alphabet City", "borough": "Manhattan", "lat": 40.7265, "lon": -73.9815},
    {"zone_id": 48, "zone_name": "Clinton East", "borough": "Manhattan", "lat": 40.7623, "lon": -73.9874},
    {"zone_id": 79, "zone_name": "East Village", "borough": "Manhattan", "lat": 40.7265, "lon": -73.9815},
    {"zone_id": 87, "zone_name": "Financial District North", "borough": "Manhattan", "lat": 40.7094, "lon": -74.0090},
    {"zone_id": 90, "zone_name": "Flatiron", "borough": "Manhattan", "lat": 40.7411, "lon": -73.9897},
    {"zone_id": 100, "zone_name": "Garment District", "borough": "Manhattan", "lat": 40.7536, "lon": -73.9910},
    {"zone_id": 107, "zone_name": "Gramercy", "borough": "Manhattan", "lat": 40.7368, "lon": -73.9845},
    {"zone_id": 113, "zone_name": "Greenwich Village North", "borough": "Manhattan", "lat": 40.7336, "lon": -73.9991},
    {"zone_id": 125, "zone_name": "Hudson Sq", "borough": "Manhattan", "lat": 40.7270, "lon": -74.0080},
    {"zone_id": 137, "zone_name": "Kips Bay", "borough": "Manhattan", "lat": 40.7420, "lon": -73.9780},
    {"zone_id": 140, "zone_name": "Lenox Hill East", "borough": "Manhattan", "lat": 40.7680, "lon": -73.9580},
    {"zone_id": 142, "zone_name": "Lincoln Square East", "borough": "Manhattan", "lat": 40.7736, "lon": -73.9832},
    {"zone_id": 148, "zone_name": "Lower East Side", "borough": "Manhattan", "lat": 40.7150, "lon": -73.9843},
    {"zone_id": 158, "zone_name": "Meatpacking/West Village West", "borough": "Manhattan", "lat": 40.7395, "lon": -74.0080},
    {"zone_id": 161, "zone_name": "Midtown Center", "borough": "Manhattan", "lat": 40.7549, "lon": -73.9840},
    {"zone_id": 162, "zone_name": "Midtown East", "borough": "Manhattan", "lat": 40.7527, "lon": -73.9720},
    {"zone_id": 163, "zone_name": "Midtown North", "borough": "Manhattan", "lat": 40.7620, "lon": -73.9790},
    {"zone_id": 164, "zone_name": "Midtown South", "borough": "Manhattan", "lat": 40.7480, "lon": -73.9850},
    {"zone_id": 170, "zone_name": "Murray Hill", "borough": "Manhattan", "lat": 40.7480, "lon": -73.9780},
    {"zone_id": 186, "zone_name": "Penn Station/Madison Sq West", "borough": "Manhattan", "lat": 40.7506, "lon": -73.9936},
    {"zone_id": 211, "zone_name": "SoHo", "borough": "Manhattan", "lat": 40.7233, "lon": -74.0020},
    {"zone_id": 230, "zone_name": "Times Sq/Theatre District", "borough": "Manhattan", "lat": 40.7580, "lon": -73.9855},
    {"zone_id": 231, "zone_name": "TriBeCa/Civic Center", "borough": "Manhattan", "lat": 40.7163, "lon": -74.0086},
    {"zone_id": 234, "zone_name": "Union Sq", "borough": "Manhattan", "lat": 40.7359, "lon": -73.9906},
    {"zone_id": 236, "zone_name": "Upper East Side North", "borough": "Manhattan", "lat": 40.7780, "lon": -73.9550},
    {"zone_id": 237, "zone_name": "Upper East Side South", "borough": "Manhattan", "lat": 40.7680, "lon": -73.9620},
    {"zone_id": 238, "zone_name": "Upper West Side North", "borough": "Manhattan", "lat": 40.7900, "lon": -73.9700},
    {"zone_id": 239, "zone_name": "Upper West Side South", "borough": "Manhattan", "lat": 40.7800, "lon": -73.9800},
    {"zone_id": 246, "zone_name": "West Chelsea/Hudson Yards", "borough": "Manhattan", "lat": 40.7530, "lon": -74.0020},
    {"zone_id": 249, "zone_name": "West Village", "borough": "Manhattan", "lat": 40.7336, "lon": -74.0027},
    {"zone_id": 261, "zone_name": "World Trade Center", "borough": "Manhattan", "lat": 40.7118, "lon": -74.0131},
    {"zone_id": 17, "zone_name": "Bedford", "borough": "Brooklyn", "lat": 40.6872, "lon": -73.9418},
    {"zone_id": 33, "zone_name": "Brooklyn Heights", "borough": "Brooklyn", "lat": 40.6960, "lon": -73.9936},
    {"zone_id": 61, "zone_name": "DUMBO/Vinegar Hill", "borough": "Brooklyn", "lat": 40.7033, "lon": -73.9880},
    {"zone_id": 80, "zone_name": "Fort Greene", "borough": "Brooklyn", "lat": 40.6892, "lon": -73.9760},
    {"zone_id": 188, "zone_name": "Park Slope", "borough": "Brooklyn", "lat": 40.6720, "lon": -73.9777},
    {"zone_id": 256, "zone_name": "Williamsburg (North Side)", "borough": "Brooklyn", "lat": 40.7180, "lon": -73.9570},
    {"zone_id": 7, "zone_name": "Astoria", "borough": "Queens", "lat": 40.7720, "lon": -73.9300},
    {"zone_id": 138, "zone_name": "LaGuardia Airport", "borough": "Queens", "lat": 40.7769, "lon": -73.8740},
    {"zone_id": 132, "zone_name": "JFK Airport", "borough": "Queens", "lat": 40.6413, "lon": -73.7781},
    {"zone_id": 145, "zone_name": "Long Island City/Hunters Point", "borough": "Queens", "lat": 40.7420, "lon": -73.9580},
]

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * asin(sqrt(a)) * 6371

def find_nearest_taxi_zone(lat, lon):
    if lat is None or lon is None:
        return {"taxi_zone_id": None, "taxi_zone_name": "Unknown", "borough": "Unknown"}
    nearest, min_dist = None, float('inf')
    for zone in NYC_TAXI_ZONES:
        dist = haversine(lon, lat, zone["lon"], zone["lat"])
        if dist < min_dist:
            min_dist, nearest = dist, zone
    if nearest:
        return {"taxi_zone_id": nearest["zone_id"], "taxi_zone_name": nearest["zone_name"], "borough": nearest["borough"]}
    return {"taxi_zone_id": None, "taxi_zone_name": "Unknown", "borough": "Unknown"}


# ==============================================================================
# OPENAQ API FUNCTIONS
# ==============================================================================
def get_locations_with_sensors(api_key: str, limit: int = 50) -> List[Dict]:
    """Fetch OpenAQ locations with sensors near NYC."""
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        print("   [WARNING] API Key is not set!")
        return []
    
    url = f"{Config.OPENAQ_BASE_URL}/locations"
    headers = {"X-API-Key": api_key}
    params = {"coordinates": f"{Config.NYC_LAT},{Config.NYC_LON}", "radius": 25000, "limit": limit}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])
        print(f"   [OK] Received {len(results)} locations from OpenAQ API")
    except Exception as e:
        print(f"   [ERROR] API Error: {e}")
        return []
    
    locations = []
    for loc in results:
        coords = loc.get("coordinates", {})
        lat, lon = coords.get("latitude"), coords.get("longitude")
        zone_info = find_nearest_taxi_zone(lat, lon)
        
        for sensor in loc.get("sensors", []):
            locations.append({
                "location_id": loc.get("id"),
                "location_name": loc.get("name", "Unknown"),
                "sensor_id": sensor.get("id"),
                "parameter": sensor.get("parameter", {}).get("name", "unknown"),
                "latitude": lat, "longitude": lon,
                "taxi_zone_id": zone_info["taxi_zone_id"],
                "taxi_zone_name": zone_info["taxi_zone_name"],
                "borough": zone_info["borough"]
            })
    
    unique = {l["location_id"]: l["taxi_zone_name"] for l in locations}
    print(f"   [INFO] Mapped to taxi zones: {len(unique)} stations")
    return locations


def fetch_measurements(api_key: str, sensors: List[Dict], days: int = 90) -> List[Dict]:
    """Fetch measurements for the last N days (default 90 for better overlap)."""
    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    target_params = ["pm25", "no2", "o3", "pm2.5"]
    filtered = [s for s in sensors if s["parameter"].lower() in target_params][:30]
    
    print(f"   [INFO] Fetching data for {len(filtered)} sensors ({date_from} -> {date_to})...")
    
    records = []
    for sensor in filtered:
        url = f"{Config.OPENAQ_BASE_URL}/sensors/{sensor['sensor_id']}/days"
        headers = {"X-API-Key": api_key}
        params = {"date_from": date_from, "date_to": date_to, "limit": 100}
        
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=5)
            if resp.status_code == 200:
                for r in resp.json().get("results", []):
                    val = r.get("value")
                    dt = r.get("period", {}).get("datetimeFrom", {}).get("local", "")[:10]
                    if val is not None and dt:
                        param = sensor["parameter"].lower()
                        if param == "pm2.5": param = "pm25"
                        records.append({
                            "date": dt,
                            "location_id": sensor["location_id"],
                            "taxi_zone_id": sensor["taxi_zone_id"],
                            "parameter": param,
                            "value": round(float(val), 2)
                        })
        except requests.RequestException:
            continue
    
    print(f"   [OK] Collected {len(records)} measurements")
    return records


def build_dim_zone(spark: SparkSession, sensors_data: List[Dict]) -> DataFrame:
    unique = {}
    for s in sensors_data:
        if s["location_id"] not in unique:
            unique[s["location_id"]] = s
    
    data = list(unique.values())
    if not data:
        return spark.createDataFrame([], StructType([
            StructField("zone_key", IntegerType()), StructField("zone_id", IntegerType()),
            StructField("zone_name", StringType()), StructField("borough", StringType()),
            StructField("taxi_zone_id", IntegerType()), StructField("taxi_zone_name", StringType())
        ]))
    
    df = spark.createDataFrame(data)
    return df.select(
        col("location_id").cast("int").alias("zone_key"),
        col("location_id").cast("int").alias("zone_id"),
        col("location_name").alias("zone_name"),
        col("borough"),
        col("taxi_zone_id").cast("int"),
        col("taxi_zone_name"),
        col("latitude").cast("double"),
        col("longitude").cast("double")
    )


def build_fact_air_quality(spark: SparkSession, measurements: List[Dict]) -> DataFrame:
    schema = StructType([
        StructField("date", StringType()), StructField("location_id", IntegerType()),
        StructField("taxi_zone_id", IntegerType()), StructField("parameter", StringType()),
        StructField("value", DoubleType())
    ])
    
    if not measurements:
        print("   [WARNING] No data to write")
        return spark.createDataFrame([], schema)
    
    df = spark.createDataFrame(measurements, schema)
    pivoted = df.groupBy("date", "location_id", "taxi_zone_id").pivot("parameter").agg(F.round(F.avg("value"), 1))
    
    for c in ["pm25", "no2", "o3"]:
        if c not in pivoted.columns:
            pivoted = pivoted.withColumn(c, lit(None).cast("double"))
    
    return pivoted.withColumn(
        "date_key", (year(col("date")) * 10000 + month(col("date")) * 100 + dayofmonth(col("date"))).cast("int")
    ).withColumn(
        "pm25", when(col("pm25") < 0, lit(None)).otherwise(col("pm25"))
    ).withColumn(
        "aqi",
        when(col("pm25").isNull(), lit(None))
        .when(col("pm25") <= 12, (col("pm25")/12*50).cast("int"))
        .when(col("pm25") <= 35.4, (50 + (col("pm25") - 12) / 23.4 * 50).cast("int"))
        .otherwise(100)
    ).select("date_key", "location_id", "taxi_zone_id",
             col("pm25").cast("decimal(5,1)"),
             col("no2").cast("decimal(5,1)"),
             col("o3").cast("decimal(5,1)"), "aqi")


# ==============================================================================
# ORCHESTRATION
# ==============================================================================
def run_bronze_to_silver(spark: SparkSession) -> dict:
    results = {}
    
    taxi_raw = read_taxi_bronze(spark)
    taxi_clean = clean_taxi(taxi_raw)
    results["cleaned_taxi_trips"] = write_delta(taxi_clean, f"{Config.SILVER_PATH}/Tables/cleaned_taxi_trips")
    log_success("cleaned_taxi_trips", results["cleaned_taxi_trips"])
    
    gdp_raw = read_gdp_bronze(spark)
    gdp_clean = clean_gdp(gdp_raw)
    results["cleaned_gdp"] = write_delta(gdp_clean, f"{Config.SILVER_PATH}/Tables/cleaned_gdp")
    log_success("cleaned_gdp", results["cleaned_gdp"])
    
    fx_raw = read_fx_bronze(spark)
    fx_clean = clean_fx(fx_raw)
    results["cleaned_fx_rates"] = write_delta(fx_clean, f"{Config.SILVER_PATH}/Tables/cleaned_fx_rates")
    log_success("cleaned_fx_rates", results["cleaned_fx_rates"])
    
    return results


def run_silver_to_gold(spark: SparkSession) -> dict:
    results = {}
    gold_path = f"{Config.GOLD_PATH}/Tables"
    
    dim_date = build_dim_date(spark)
    results["DimDate"] = write_delta(dim_date, f"{gold_path}/DimDate")
    log_success("DimDate", results["DimDate"])
    
    dim_fx = build_dim_fx(spark)
    results["DimFX"] = write_delta(dim_fx, f"{gold_path}/DimFX")
    log_success("DimFX", results["DimFX"])
    
    dim_gdp = build_dim_gdp(spark)
    results["DimGDP"] = write_delta(dim_gdp, f"{gold_path}/DimGDP")
    log_success("DimGDP", results["DimGDP"])
    
    fact_taxi = build_fact_taxi_daily(spark)
    results["FactTaxiDaily"] = write_delta(fact_taxi, f"{gold_path}/FactTaxiDaily")
    log_success("FactTaxiDaily", results["FactTaxiDaily"])
    
    return results


def run_openaq_to_gold(spark: SparkSession) -> dict:
    results = {}
    gold_path = f"{Config.GOLD_PATH}/Tables"
    
    print("   [INFO] Connecting to OpenAQ API (with zone mapping)...")
    
    sensors = get_locations_with_sensors(Config.OPENAQ_API_KEY)
    if not sensors:
        print("   [ERROR] No sensors found!")
        return results
    
    dim_zone = build_dim_zone(spark, sensors)
    results["DimZone"] = write_delta(dim_zone, f"{gold_path}/DimZone")
    log_success("DimZone", results["DimZone"])
    
    measurements = fetch_measurements(Config.OPENAQ_API_KEY, sensors)
    
    fact_aq = build_fact_air_quality(spark, measurements)
    results["FactAirQualityDaily"] = write_delta(fact_aq, f"{gold_path}/FactAirQualityDaily")
    log_success("FactAirQualityDaily", results["FactAirQualityDaily"])
    
    return results


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
print("=" * 60)
print("STARTING COMPLETE DATA PIPELINE")
print("=" * 60)

print("\n[PHASE 1] Bronze -> Silver")
print("-" * 40)
bronze_results = run_bronze_to_silver(spark)

print("\n[PHASE 2] Silver -> Gold")
print("-" * 40)
gold_results = run_silver_to_gold(spark)

print("\n[PHASE 3] OpenAQ -> Gold (with zone mapping)")
print("-" * 40)
openaq_results = run_openaq_to_gold(spark)

print("\n" + "=" * 60)
print("PIPELINE COMPLETE!")
print("=" * 60)

print("\n[VERIFICATION]")
df = spark.read.format("delta").load(f"{Config.GOLD_PATH}/Tables/FactAirQualityDaily")
print(f"   FactAirQualityDaily: {df.count()} rows total")
print(f"   Rows with PM2.5 data: {df.filter(col('pm25').isNotNull()).count()}")
print(f"   Rows with taxi_zone_id: {df.filter(col('taxi_zone_id').isNotNull()).count()}")
