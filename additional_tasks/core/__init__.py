# ==============================================================================
# Core Module - Shared Configuration, Logging, and Utilities
#
# This module provides enterprise-grade infrastructure components used across
# all integration services: Weather, Great Expectations, and Power Automate.
#
# Design Patterns Used:
#   - Singleton Pattern for configuration
#   - Factory Pattern for logger creation
#   - Decorator Pattern for retry logic
#
# Author: Senior Data Engineer
# ==============================================================================

import functools
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

# Try to load dotenv for environment variables
try:
    from dotenv import load_dotenv
    # Load .env from the additional_tasks directory
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
except ImportError:
    pass  # dotenv not installed, rely on system environment


# ==============================================================================
# CONFIGURATION (Singleton Pattern)
# ==============================================================================

@dataclass
class InfluxDBConfig:
    """InfluxDB connection configuration."""
    url: str = field(default_factory=lambda: os.getenv("INFLUXDB_URL", "http://localhost:8086"))
    org: str = field(default_factory=lambda: os.getenv("INFLUXDB_ORG", "fabric_org"))
    bucket: str = field(default_factory=lambda: os.getenv("INFLUXDB_BUCKET", "weather_data"))
    token: str = field(default_factory=lambda: os.getenv("INFLUXDB_TOKEN", "fabric-weather-token-2024"))


@dataclass
class TelegramConfig:
    """Telegram bot configuration."""
    bot_token: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    bot_name: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_NAME", "ItransitionProjectBot"))
    
    @property
    def is_configured(self) -> bool:
        return bool(self.bot_token)
    
    @property
    def api_url(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}"


@dataclass
class WeatherConfig:
    """Weather API configuration."""
    latitude: float = field(default_factory=lambda: float(os.getenv("WEATHER_LATITUDE", "40.7128")))
    longitude: float = field(default_factory=lambda: float(os.getenv("WEATHER_LONGITUDE", "-74.0060")))
    location_name: str = field(default_factory=lambda: os.getenv("WEATHER_LOCATION_NAME", "New York City"))
    api_url: str = "https://api.open-meteo.com/v1/forecast"


@dataclass 
class DataConfig:
    """Data paths configuration."""
    gold_layer_path: str = field(default_factory=lambda: os.getenv(
        "DATA_GOLD_LAYER_PATH", 
        "d:/itransition_task_final/data/gold/FactTaxiDaily.parquet"
    ))
    export_output_dir: str = field(default_factory=lambda: os.getenv(
        "EXPORT_OUTPUT_DIR",
        "d:/itransition_task_final/additional_tasks/power_automate/exports"
    ))
    onedrive_sync_path: str = field(default_factory=lambda: os.getenv(
        "ONEDRIVE_SYNC_PATH",
        "C:/Users/Taneka/OneDrive/FabricReports"
    ))


class AppConfig:
    """
    Application configuration singleton.
    
    Usage:
        config = AppConfig()
        print(config.influxdb.url)
        print(config.telegram.bot_token)
    """
    _instance: Optional['AppConfig'] = None
    
    def __new__(cls) -> 'AppConfig':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        self.influxdb = InfluxDBConfig()
        self.telegram = TelegramConfig()
        self.weather = WeatherConfig()
        self.data = DataConfig()
        self.log_level = os.getenv("LOG_LEVEL", "INFO")


# ==============================================================================
# LOGGING (Factory Pattern)
# ==============================================================================

class LoggerFactory:
    """
    Factory for creating configured loggers.
    
    Features:
    - Consistent format across all modules
    - Windows encoding fix (UTF-8 with error replacement)
    - Configurable log level via environment
    """
    
    _initialized: bool = False
    _log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    @classmethod
    def setup(cls, level: Optional[str] = None) -> None:
        """Initialize logging for the application."""
        if cls._initialized:
            return
        
        # Fix Windows console encoding
        if sys.platform == 'win32':
            try:
                sys.stdout.reconfigure(encoding='utf-8', errors='replace')
                sys.stderr.reconfigure(encoding='utf-8', errors='replace')
            except Exception:
                pass
        
        # Configure root logger
        log_level = level or AppConfig().log_level
        logging.basicConfig(
            level=getattr(logging, log_level.upper(), logging.INFO),
            format=cls._log_format,
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        
        cls._initialized = True
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get a configured logger for a module."""
        cls.setup()
        return logging.getLogger(name)


# ==============================================================================
# RETRY DECORATOR (Decorator Pattern)
# ==============================================================================

T = TypeVar('T')


def with_retry(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    exponential_backoff: bool = True,
    exceptions: tuple = (Exception,)
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying functions on failure.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay_seconds: Initial delay between retries
        exponential_backoff: Whether to double delay after each attempt
        exceptions: Tuple of exception types to catch and retry
    
    Usage:
        @with_retry(max_attempts=3, delay_seconds=1.0)
        def fetch_data():
            return requests.get(url)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            logger = LoggerFactory.get_logger(func.__module__)
            last_exception: Optional[Exception] = None
            current_delay = delay_seconds
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        if exponential_backoff:
                            current_delay *= 2
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
            
            raise last_exception  # type: ignore
        return wrapper
    return decorator


# ==============================================================================
# ERROR HANDLING
# ==============================================================================

class IntegrationError(Exception):
    """Base exception for integration errors."""
    pass


class ConfigurationError(IntegrationError):
    """Raised when configuration is invalid or missing."""
    pass


class DataSourceError(IntegrationError):
    """Raised when data source is unavailable or returns bad data."""
    pass


class ExternalServiceError(IntegrationError):
    """Raised when external service (API, database) fails."""
    pass


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def ensure_directory(path: str) -> Path:
    """Ensure a directory exists, create if necessary."""
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def safe_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely get nested dictionary values."""
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        else:
            return default
    return result


# Initialize logging on import
LoggerFactory.setup()
