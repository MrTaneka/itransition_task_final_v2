# ==============================================================================
# Configuration Module for Weather Ingestion Service
# 
# This module centralizes all configuration for the weather data pipeline.
# Following 12-factor app principles: configuration via environment variables.
# ==============================================================================

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class WeatherAPIConfig:
    """Configuration for Open-Meteo Weather API."""
    
    base_url: str = "https://api.open-meteo.com/v1/forecast"
    # NYC coordinates
    latitude: float = 40.7128
    longitude: float = -74.0060
    # API parameters
    hourly_params: tuple = (
        "temperature_2m",
        "relative_humidity_2m", 
        "precipitation",
        "wind_speed_10m",
        "pressure_msl",
        "cloud_cover"
    )
    past_days: int = 7
    forecast_days: int = 1
    timezone: str = "America/New_York"


@dataclass(frozen=True)
class InfluxDBConfig:
    """Configuration for InfluxDB connection."""
    
    url: str = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token: str = os.getenv("INFLUXDB_TOKEN", "fabric-weather-token-2024")
    org: str = os.getenv("INFLUXDB_ORG", "fabric_org")
    bucket: str = os.getenv("INFLUXDB_BUCKET", "weather_data")


@dataclass(frozen=True)
class FabricConfig:
    """Configuration for Microsoft Fabric SQL Endpoint connection."""
    
    server: str = os.getenv(
        "FABRIC_SQL_SERVER", 
        "your-workspace.sql.fabric.microsoft.com"
    )
    database: str = os.getenv("FABRIC_DATABASE", "Gold")
    # Authentication method: ActiveDirectoryInteractive or ServicePrincipal
    auth_method: str = os.getenv("FABRIC_AUTH_METHOD", "ActiveDirectoryInteractive")


@dataclass(frozen=True)
class SchedulerConfig:
    """Configuration for job scheduler."""
    
    # Weather data refresh interval in minutes
    weather_interval_minutes: int = int(os.getenv("WEATHER_INTERVAL_MINUTES", "60"))
    # Enable/disable scheduler
    enabled: bool = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"


# Global configuration instances
WEATHER_API = WeatherAPIConfig()
INFLUXDB = InfluxDBConfig()
FABRIC = FabricConfig()
SCHEDULER = SchedulerConfig()


def get_influxdb_connection_string() -> str:
    """Generate InfluxDB connection string for logging purposes."""
    return f"{INFLUXDB.url} (org: {INFLUXDB.org}, bucket: {INFLUXDB.bucket})"


def validate_config() -> bool:
    """
    Validate that all required configuration is present.
    
    Returns:
        bool: True if configuration is valid
        
    Raises:
        ValueError: If required configuration is missing
    """
    errors = []
    
    if INFLUXDB.token == "YOUR_TOKEN_HERE":
        errors.append("INFLUXDB_TOKEN must be set")
    
    if errors:
        raise ValueError(f"Configuration errors: {', '.join(errors)}")
    
    return True
