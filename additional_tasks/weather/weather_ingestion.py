# ==============================================================================
# Weather Data Ingestion Service
# 
# This module fetches weather data from Open-Meteo API and writes it to InfluxDB.
# Designed for periodic execution to build a time-series of NYC weather data
# that can be correlated with taxi and air quality metrics from Microsoft Fabric.
#
# Author: Senior Data Engineer
# Best Practices Applied:
# - Type hints throughout
# - Comprehensive error handling
# - Retry logic for API calls
# - Structured logging
# - Clean separation of concerns
# ==============================================================================

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import INFLUXDB, WEATHER_API, get_influxdb_connection_string

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('weather_ingestion.log')
    ]
)
logger = logging.getLogger(__name__)


def create_session_with_retry(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (500, 502, 503, 504)
) -> requests.Session:
    """
    Create a requests session with retry logic.
    
    Args:
        retries: Number of retry attempts
        backoff_factor: Multiplier for exponential backoff
        status_forcelist: HTTP status codes that trigger retry
        
    Returns:
        Configured requests.Session object
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_weather_data() -> Optional[Dict[str, Any]]:
    """
    Fetch weather data from Open-Meteo API.
    
    Returns:
        Dictionary containing weather data or None if request fails
    """
    params = {
        "latitude": WEATHER_API.latitude,
        "longitude": WEATHER_API.longitude,
        "hourly": ",".join(WEATHER_API.hourly_params),
        "past_days": WEATHER_API.past_days,
        "forecast_days": WEATHER_API.forecast_days,
        "timezone": WEATHER_API.timezone
    }
    
    logger.info(f"Fetching weather data for NYC ({WEATHER_API.latitude}, {WEATHER_API.longitude})")
    
    try:
        session = create_session_with_retry()
        response = session.get(
            WEATHER_API.base_url,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully fetched {len(data.get('hourly', {}).get('time', []))} hourly records")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch weather data: {e}")
        return None


def transform_weather_data(raw_data: Dict[str, Any]) -> List[Point]:
    """
    Transform raw API response into InfluxDB Points.
    
    Args:
        raw_data: Raw JSON response from Open-Meteo API
        
    Returns:
        List of InfluxDB Point objects ready for writing
    """
    points = []
    hourly = raw_data.get("hourly", {})
    times = hourly.get("time", [])
    
    for i, time_str in enumerate(times):
        try:
            # Parse timestamp
            timestamp = datetime.fromisoformat(time_str)
            
            # Create point with all measurements
            point = Point("nyc_weather") \
                .tag("city", "New York") \
                .tag("source", "open-meteo") \
                .time(timestamp, WritePrecision.S)
            
            # Add all available fields
            if "temperature_2m" in hourly and hourly["temperature_2m"][i] is not None:
                point = point.field("temperature_c", float(hourly["temperature_2m"][i]))
            
            if "relative_humidity_2m" in hourly and hourly["relative_humidity_2m"][i] is not None:
                point = point.field("humidity_pct", float(hourly["relative_humidity_2m"][i]))
            
            if "precipitation" in hourly and hourly["precipitation"][i] is not None:
                point = point.field("precipitation_mm", float(hourly["precipitation"][i]))
            
            if "wind_speed_10m" in hourly and hourly["wind_speed_10m"][i] is not None:
                point = point.field("wind_speed_kmh", float(hourly["wind_speed_10m"][i]))
            
            if "pressure_msl" in hourly and hourly["pressure_msl"][i] is not None:
                point = point.field("pressure_hpa", float(hourly["pressure_msl"][i]))
            
            if "cloud_cover" in hourly and hourly["cloud_cover"][i] is not None:
                point = point.field("cloud_cover_pct", float(hourly["cloud_cover"][i]))
            
            points.append(point)
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Skipping record at index {i}: {e}")
            continue
    
    logger.info(f"Transformed {len(points)} data points")
    return points


def write_to_influxdb(points: List[Point]) -> bool:
    """
    Write data points to InfluxDB.
    
    Args:
        points: List of InfluxDB Point objects
        
    Returns:
        True if write was successful, False otherwise
    """
    logger.info(f"Connecting to InfluxDB: {get_influxdb_connection_string()}")
    
    try:
        with InfluxDBClient(
            url=INFLUXDB.url,
            token=INFLUXDB.token,
            org=INFLUXDB.org
        ) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(bucket=INFLUXDB.bucket, record=points)
            
        logger.info(f"Successfully wrote {len(points)} points to InfluxDB")
        return True
        
    except Exception as e:
        logger.error(f"Failed to write to InfluxDB: {e}")
        return False


def run_ingestion() -> Dict[str, Any]:
    """
    Run the complete weather data ingestion pipeline.
    
    Returns:
        Dictionary with pipeline execution results
    """
    start_time = datetime.now()
    results = {
        "start_time": start_time.isoformat(),
        "status": "failed",
        "records_fetched": 0,
        "records_written": 0,
        "errors": []
    }
    
    logger.info("=" * 60)
    logger.info("Starting weather data ingestion pipeline")
    logger.info("=" * 60)
    
    # Step 1: Fetch data
    raw_data = fetch_weather_data()
    if not raw_data:
        results["errors"].append("Failed to fetch weather data")
        return results
    
    results["records_fetched"] = len(raw_data.get("hourly", {}).get("time", []))
    
    # Step 2: Transform data
    points = transform_weather_data(raw_data)
    if not points:
        results["errors"].append("No data points after transformation")
        return results
    
    # Step 3: Write to InfluxDB
    if write_to_influxdb(points):
        results["records_written"] = len(points)
        results["status"] = "success"
    else:
        results["errors"].append("Failed to write to InfluxDB")
    
    # Calculate duration
    end_time = datetime.now()
    results["end_time"] = end_time.isoformat()
    results["duration_seconds"] = (end_time - start_time).total_seconds()
    
    logger.info(f"Pipeline completed: {results['status']}")
    logger.info(f"Duration: {results['duration_seconds']:.2f} seconds")
    logger.info("=" * 60)
    
    return results


if __name__ == "__main__":
    # Run single ingestion
    result = run_ingestion()
    
    if result["status"] == "success":
        print(f"\n[OK] Ingestion successful!")
        print(f"   Records written: {result['records_written']}")
    else:
        print(f"\n[ERROR] Ingestion failed!")
        print(f"   Errors: {result['errors']}")
        sys.exit(1)
