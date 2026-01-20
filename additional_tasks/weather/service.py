# ==============================================================================
# Weather Ingestion Service
#
# Fetches weather data from Open-Meteo API and stores in InfluxDB time-series
# database. Designed for periodic execution via scheduler or cron job.
# ==============================================================================

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import (
    AppConfig,
    DataSourceError,
    ExternalServiceError,
    LoggerFactory,
    with_retry,
)

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError:
    print("Please install influxdb-client: pip install influxdb-client")
    sys.exit(1)


logger = LoggerFactory.get_logger(__name__)


@dataclass
class WeatherRecord:
    """Represents a single weather measurement."""
    timestamp: datetime
    temperature_c: float
    humidity_percent: float
    wind_speed_kmh: float
    precipitation_mm: float
    location: str
    
    def to_influx_point(self) -> Point:
        """Convert to InfluxDB Point for writing."""
        return (
            Point("nyc_weather")
            .tag("city", "New York")
            .tag("source", "open-meteo")
            .tag("location", self.location)
            .field("temperature_c", self.temperature_c)
            .field("humidity_pct", self.humidity_percent)
            .field("wind_speed_kmh", self.wind_speed_kmh)
            .field("precipitation_mm", self.precipitation_mm)
            .time(self.timestamp, WritePrecision.S)
        )


class InfluxDBRepository:
    """
    Repository for InfluxDB operations.
    
    Encapsulates all database interactions following Repository Pattern.
    This allows easy mocking for tests and swapping implementations.
    """
    
    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or AppConfig()
        self._client: Optional[InfluxDBClient] = None
        self._write_api = None
    
    def connect(self) -> None:
        """Establish connection to InfluxDB."""
        if self._client is not None:
            return
        
        cfg = self.config.influxdb
        logger.info(f"Connecting to InfluxDB at {cfg.url}")
        
        self._client = InfluxDBClient(
            url=cfg.url,
            token=cfg.token,
            org=cfg.org
        )
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        
        if not self._client.ping():
            raise ExternalServiceError(f"Cannot connect to InfluxDB at {cfg.url}")
        
        logger.info("Successfully connected to InfluxDB")
    
    def disconnect(self) -> None:
        """Close connection to InfluxDB."""
        if self._client:
            self._client.close()
            self._client = None
            self._write_api = None
    
    @with_retry(max_attempts=3, delay_seconds=2.0)
    def write_records(self, records: List[WeatherRecord]) -> int:
        """
        Write weather records to InfluxDB.
        
        Returns:
            Number of records written
        """
        if not self._write_api:
            raise ExternalServiceError("Not connected to InfluxDB")
        
        points = [record.to_influx_point() for record in records]
        
        self._write_api.write(
            bucket=self.config.influxdb.bucket,
            org=self.config.influxdb.org,
            record=points
        )
        
        logger.info(f"Wrote {len(points)} weather records to InfluxDB")
        return len(points)
    
    def __enter__(self) -> 'InfluxDBRepository':
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()



class OpenMeteoClient:
    """
    Client for Open-Meteo weather API.
    
    Open-Meteo provides free weather data without API key.
    https://open-meteo.com/
    """
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    
    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or AppConfig()
    
    @with_retry(max_attempts=3, delay_seconds=1.0)
    def fetch_historical_weather(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Fetch historical hourly weather data.
        
        Args:
            days_back: Number of days of historical data to fetch
            
        Returns:
            List of hourly weather data points
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        params = {
            "latitude": self.config.weather.latitude,
            "longitude": self.config.weather.longitude,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "timezone": "auto"
        }
        
        logger.info(
            f"Fetching weather data for {self.config.weather.location_name} "
            f"({start_date.date()} to {end_date.date()})"
        )
        
        response = requests.get(self.BASE_URL, params=params, timeout=30)
        
        if response.status_code != 200:
            raise DataSourceError(
                f"Open-Meteo API error: {response.status_code} - {response.text}"
            )
        
        return response.json()
    
    def parse_response(self, data: Dict[str, Any]) -> List[WeatherRecord]:
        """Parse API response into WeatherRecord objects."""
        hourly = data.get("hourly", {})
        
        timestamps = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        humidity = hourly.get("relative_humidity_2m", [])
        wind_speed = hourly.get("wind_speed_10m", [])
        precipitation = hourly.get("precipitation", [])
        
        records = []
        location = self.config.weather.location_name
        
        for i, ts in enumerate(timestamps):
            try:
                records.append(WeatherRecord(
                    timestamp=datetime.fromisoformat(ts),
                    temperature_c=float(temperatures[i]) if i < len(temperatures) else 0.0,
                    humidity_percent=float(humidity[i]) if i < len(humidity) else 0.0,
                    wind_speed_kmh=float(wind_speed[i]) if i < len(wind_speed) else 0.0,
                    precipitation_mm=float(precipitation[i]) if i < len(precipitation) else 0.0,
                    location=location
                ))
            except (ValueError, IndexError) as e:
                logger.warning(f"Skipping invalid record at index {i}: {e}")
        
        logger.info(f"Parsed {len(records)} weather records")
        return records


class WeatherIngestionService:
    """
    Main service for weather data ingestion.
    
    Orchestrates fetching data from API and storing in database.
    Uses dependency injection for testability.
    """
    
    def __init__(
        self,
        api_client: Optional[OpenMeteoClient] = None,
        repository: Optional[InfluxDBRepository] = None,
        config: Optional[AppConfig] = None
    ):
        self.config = config or AppConfig()
        self.api_client = api_client or OpenMeteoClient(self.config)
        self.repository = repository or InfluxDBRepository(self.config)
    
    def run_ingestion(self, days_back: int = 7) -> int:
        """
        Execute weather data ingestion pipeline.
        
        Args:
            days_back: Number of days to fetch
            
        Returns:
            Number of records ingested
        """
        logger.info("=" * 60)
        logger.info("Starting Weather Ingestion Pipeline")
        logger.info("=" * 60)
        
        try:
            # Fetch data from API
            raw_data = self.api_client.fetch_historical_weather(days_back)
            records = self.api_client.parse_response(raw_data)
            
            if not records:
                logger.warning("No weather records to ingest")
                return 0
            
            # Store in database
            with self.repository as repo:
                count = repo.write_records(records)
            
            logger.info("=" * 60)
            logger.info(f"Ingestion complete: {count} records written")
            logger.info("=" * 60)
            
            return count
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise
    
    def run_daemon(self, interval_hours: int = 1) -> None:
        """
        Run ingestion continuously as a daemon.
        
        Args:
            interval_hours: Hours between ingestion runs
        """
        logger.info(f"Starting daemon mode (interval: {interval_hours}h)")
        
        while True:
            try:
                self.run_ingestion(days_back=1)
            except Exception as e:
                logger.error(f"Daemon run failed: {e}")
            
            sleep_seconds = interval_hours * 3600
            logger.info(f"Sleeping for {interval_hours} hour(s)...")
            time.sleep(sleep_seconds)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Weather Data Ingestion Service"
    )
    parser.add_argument(
        "--daemon", 
        action="store_true",
        help="Run continuously as a daemon"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=1,
        help="Interval between runs in daemon mode (hours)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days of historical data to fetch"
    )
    
    args = parser.parse_args()
    
    service = WeatherIngestionService()
    
    if args.daemon:
        service.run_daemon(interval_hours=args.interval)
    else:
        count = service.run_ingestion(days_back=args.days)
        print(f"\n[SUCCESS] Ingested {count} weather records")


if __name__ == "__main__":
    main()
