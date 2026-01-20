# ==============================================================================
# Data Quality Validation Service
#
# Implements Great Expectations-inspired data quality checks with Telegram bot
# integration for user-friendly triggering and notifications.
#
# Architecture:
#   - Strategy Pattern: Each expectation is a strategy
#   - Observer Pattern: Notifiers observe validation results  
#   - Service Pattern: ValidationService orchestrates execution
#
# Usage:
#   python -m great_expectations.service          # Run validation once
#   python -m great_expectations.service --bot    # Start Telegram bot
#
# Author: Senior Data Engineer
# ==============================================================================

import argparse
import asyncio
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import AppConfig, DataSourceError, LoggerFactory, with_retry

logger = LoggerFactory.get_logger(__name__)


# ==============================================================================
# DATA MODELS
# ==============================================================================

@dataclass
class ExpectationResult:
    """Result of a single expectation check."""
    name: str
    passed: bool
    details: str = ""
    column: Optional[str] = None


@dataclass
class ValidationReport:
    """Complete validation report."""
    suite_name: str
    timestamp: datetime
    row_count: int
    results: List[ExpectationResult] = field(default_factory=list)
    
    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)
    
    @property
    def failed_count(self) -> int:
        return len(self.results) - self.passed_count
    
    @property
    def success_rate(self) -> float:
        if not self.results:
            return 0.0
        return (self.passed_count / len(self.results)) * 100
    
    @property
    def is_success(self) -> bool:
        return self.failed_count == 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "suite_name": self.suite_name,
            "timestamp": self.timestamp.isoformat(),
            "row_count": self.row_count,
            "total_checks": len(self.results),
            "passed": self.passed_count,
            "failed": self.failed_count,
            "success_rate": self.success_rate,
            "is_success": self.is_success,
            "results": [
                {"name": r.name, "passed": r.passed, "column": r.column, "details": r.details}
                for r in self.results
            ]
        }


# ==============================================================================
# STRATEGY PATTERN - Expectations
# ==============================================================================

class Expectation(ABC):
    """Base class for all expectations (Strategy Pattern)."""
    
    @abstractmethod
    def validate(self, df: pd.DataFrame) -> ExpectationResult:
        """Execute the expectation check."""
        pass


class ColumnExistsExpectation(Expectation):
    """Expect a column to exist in the dataframe."""
    
    def __init__(self, column: str):
        self.column = column
    
    def validate(self, df: pd.DataFrame) -> ExpectationResult:
        exists = self.column in df.columns
        return ExpectationResult(
            name=f"column_exists({self.column})",
            passed=exists,
            column=self.column,
            details=f"Column {'found' if exists else 'NOT found'}"
        )


class ColumnNotNullExpectation(Expectation):
    """Expect a column to have no null values."""
    
    def __init__(self, column: str, threshold: float = 0.0):
        self.column = column
        self.threshold = threshold
    
    def validate(self, df: pd.DataFrame) -> ExpectationResult:
        if self.column not in df.columns:
            return ExpectationResult(
                name=f"not_null({self.column})",
                passed=False,
                column=self.column,
                details="Column does not exist"
            )
        
        null_count = df[self.column].isnull().sum()
        null_rate = null_count / len(df) if len(df) > 0 else 0
        passed = null_rate <= self.threshold
        
        return ExpectationResult(
            name=f"not_null({self.column})",
            passed=passed,
            column=self.column,
            details=f"Null rate: {null_rate:.2%} (threshold: {self.threshold:.2%})"
        )


class ColumnValuesBetweenExpectation(Expectation):
    """Expect column values to be within a range."""
    
    def __init__(self, column: str, min_value: float, max_value: float):
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, df: pd.DataFrame) -> ExpectationResult:
        if self.column not in df.columns:
            return ExpectationResult(
                name=f"values_between({self.column})",
                passed=False,
                column=self.column,
                details="Column does not exist"
            )
        
        in_range = df[self.column].between(self.min_value, self.max_value).all()
        actual_min = df[self.column].min()
        actual_max = df[self.column].max()
        
        return ExpectationResult(
            name=f"values_between({self.column})",
            passed=in_range,
            column=self.column,
            details=f"Range [{actual_min}, {actual_max}] vs expected [{self.min_value}, {self.max_value}]"
        )


class ColumnValuesPositiveExpectation(Expectation):
    """Expect column values to be positive."""
    
    def __init__(self, column: str):
        self.column = column
    
    def validate(self, df: pd.DataFrame) -> ExpectationResult:
        if self.column not in df.columns:
            return ExpectationResult(
                name=f"positive({self.column})",
                passed=False,
                column=self.column,
                details="Column does not exist"
            )
        
        all_positive = (df[self.column] > 0).all()
        
        return ExpectationResult(
            name=f"positive({self.column})",
            passed=all_positive,
            column=self.column,
            details="All values positive" if all_positive else "Contains non-positive values"
        )


# ==============================================================================
# OBSERVER PATTERN - Notifiers
# ==============================================================================

class ValidationNotifier(ABC):
    """Base class for validation result notifiers (Observer Pattern)."""
    
    @abstractmethod
    async def notify(self, report: ValidationReport) -> bool:
        """Send notification about validation results."""
        pass


class ConsoleNotifier(ValidationNotifier):
    """Outputs validation results to console."""
    
    async def notify(self, report: ValidationReport) -> bool:
        status = "[SUCCESS]" if report.is_success else "[FAILED]"
        print(f"\n{status} Validation Report: {report.suite_name}")
        print(f"Time: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Rows checked: {report.row_count}")
        print(f"Results: {report.passed_count}/{len(report.results)} passed ({report.success_rate:.1f}%)")
        
        for result in report.results:
            icon = "[OK]" if result.passed else "[FAIL]"
            print(f"  {icon} {result.name}")
        
        return True


class TelegramNotifier(ValidationNotifier):
    """Sends validation results to Telegram."""
    
    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or AppConfig()
    
    def _format_message(self, report: ValidationReport) -> str:
        status = "[OK] PASSED" if report.is_success else "[FAIL] FAILED"
        
        message = f"""
*Data Quality Report*
{'=' * 25}

*Suite:* `{report.suite_name}`
*Time:* `{report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}`
*Rows:* `{report.row_count}`

{'=' * 25}
*Results:*
- Evaluated: {len(report.results)}
- Passed: {report.passed_count}
- Failed: {report.failed_count}
- Success Rate: {report.success_rate:.1f}%

{'=' * 25}
*Status:* {status}
"""
        
        if report.failed_count > 0:
            message += "\n*Failed Checks:*\n"
            for r in report.results:
                if not r.passed:
                    message += f"  - `{r.name}`\n"
        
        return message
    
    @with_retry(max_attempts=3, delay_seconds=1.0)
    async def notify(self, report: ValidationReport) -> bool:
        if not self.config.telegram.is_configured:
            logger.warning("Telegram not configured, skipping notification")
            return False
        
        import aiohttp
        
        message = self._format_message(report)
        url = f"{self.config.telegram.api_url}/sendMessage"
        
        # Note: In production, you'd need to store chat_id from users who messaged the bot
        # For demo, we'll just log the message
        logger.info("Telegram notification would be sent (no chat_id configured)")
        print("\n[TELEGRAM PREVIEW]")
        print(message)
        
        return True


# ==============================================================================
# DATA LOADER - Repository Pattern
# ==============================================================================

class DataLoader:
    """Loads data from various sources."""
    
    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or AppConfig()
    
    def load_from_parquet(self, path: str) -> pd.DataFrame:
        """Load data from Parquet file."""
        if Path(path).exists():
            logger.info(f"Loading data from {path}")
            return pd.read_parquet(path)
        raise DataSourceError(f"File not found: {path}")
    
    def load_gold_layer_data(self) -> pd.DataFrame:
        """Load data from Gold layer or generate sample."""
        path = self.config.data.gold_layer_path
        
        try:
            return self.load_from_parquet(path)
        except DataSourceError:
            logger.warning(f"Gold layer not found at {path}, generating sample data")
            return self._generate_sample_data()
    
    def _generate_sample_data(self) -> pd.DataFrame:
        """Generate sample taxi data for demonstration."""
        np.random.seed(42)
        n_rows = 1000
        
        return pd.DataFrame({
            'date_key': [20251001 + i % 31 for i in range(n_rows)],
            'pickup_zone_id': np.random.randint(1, 265, n_rows),
            'dropoff_zone_id': np.random.randint(1, 265, n_rows),
            'total_trips': np.random.randint(10, 1000, n_rows),
            'total_passengers': np.random.randint(10, 2000, n_rows),
            'total_distance': np.round(np.random.uniform(10, 5000, n_rows), 2),
            'total_fare': np.round(np.random.uniform(100, 50000, n_rows), 2),
            'avg_fare': np.round(np.random.uniform(10, 100, n_rows), 2)
        })


# ==============================================================================
# SERVICE PATTERN - Validation Orchestration
# ==============================================================================

class ValidationService:
    """
    Main service for data quality validation.
    
    Uses dependency injection for flexibility and testability.
    """
    
    def __init__(
        self,
        data_loader: Optional[DataLoader] = None,
        notifiers: Optional[List[ValidationNotifier]] = None,
        config: Optional[AppConfig] = None
    ):
        self.config = config or AppConfig()
        self.data_loader = data_loader or DataLoader(self.config)
        self.notifiers = notifiers or [ConsoleNotifier(), TelegramNotifier(self.config)]
        self.expectations: List[Expectation] = []
        
        # Define default expectations for taxi data
        self._setup_default_expectations()
    
    def _setup_default_expectations(self) -> None:
        """Configure default data quality expectations."""
        # Column existence checks
        required_columns = ['date_key', 'pickup_zone_id', 'dropoff_zone_id', 
                          'total_trips', 'total_fare', 'avg_fare']
        for col in required_columns:
            self.expectations.append(ColumnExistsExpectation(col))
        
        # Null checks
        for col in ['date_key', 'pickup_zone_id', 'total_trips']:
            self.expectations.append(ColumnNotNullExpectation(col))
        
        # Range checks
        self.expectations.append(ColumnValuesBetweenExpectation('total_trips', 1, 1_000_000))
        self.expectations.append(ColumnValuesBetweenExpectation('avg_fare', 1, 500))
        self.expectations.append(ColumnValuesBetweenExpectation('pickup_zone_id', 1, 265))
        
        # Positive value checks
        self.expectations.append(ColumnValuesPositiveExpectation('total_fare'))
    
    def add_expectation(self, expectation: Expectation) -> None:
        """Add a custom expectation to the suite."""
        self.expectations.append(expectation)
    
    async def run_validation(self, suite_name: str = "taxi_quality") -> ValidationReport:
        """
        Execute all validation expectations.
        
        Returns:
            ValidationReport with all results
        """
        logger.info("=" * 60)
        logger.info(f"Starting Data Quality Validation: {suite_name}")
        logger.info("=" * 60)
        
        # Load data
        df = self.data_loader.load_gold_layer_data()
        logger.info(f"Loaded {len(df)} rows for validation")
        
        # Run all expectations
        results: List[ExpectationResult] = []
        for expectation in self.expectations:
            result = expectation.validate(df)
            results.append(result)
            
            status = "[OK]" if result.passed else "[FAIL]"
            logger.info(f"  {status} {result.name}")
        
        # Create report
        report = ValidationReport(
            suite_name=suite_name,
            timestamp=datetime.now(),
            row_count=len(df),
            results=results
        )
        
        logger.info("=" * 60)
        logger.info(f"Validation complete: {report.passed_count}/{len(results)} passed")
        logger.info("=" * 60)
        
        # Notify all observers
        for notifier in self.notifiers:
            try:
                await notifier.notify(report)
            except Exception as e:
                logger.error(f"Notifier {type(notifier).__name__} failed: {e}")
        
        return report


# ==============================================================================
# TELEGRAM BOT
# ==============================================================================

class TelegramBot:
    """
    Telegram bot for triggering data quality checks.
    
    Commands:
        /start - Welcome message
        /check - Run validation
        /help - Show commands
    """
    
    def __init__(
        self,
        validation_service: Optional[ValidationService] = None,
        config: Optional[AppConfig] = None
    ):
        self.config = config or AppConfig()
        self.validation_service = validation_service or ValidationService(config=self.config)
        self.offset = 0
        self.running = False
    
    async def send_message(self, chat_id: int, text: str) -> bool:
        """Send a message to a chat."""
        import aiohttp
        
        url = f"{self.config.telegram.api_url}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            return False
    
    async def get_updates(self) -> list:
        """Get new messages from Telegram."""
        import aiohttp
        
        url = f"{self.config.telegram.api_url}/getUpdates"
        params = {"offset": self.offset, "timeout": 30}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=35)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("result", [])
        except Exception as e:
            logger.error(f"Failed to get updates: {e}")
        return []
    
    async def handle_command(self, chat_id: int, command: str, user_name: str):
        """Handle incoming bot command."""
        logger.info(f"Command from {user_name}: {command}")
        
        if command == "/start":
            await self.send_message(chat_id, f"""
*Welcome to Data Quality Bot!*

Hello {user_name}! I validate data quality.

*Commands:*
/check - Run validation
/help - Show this message
""")
        
        elif command == "/check":
            await self.send_message(chat_id, "Running data quality checks...")
            
            report = await self.validation_service.run_validation()
            
            status = "[OK] PASSED" if report.is_success else "[FAIL] FAILED"
            message = f"""
*Data Quality Report*

*Suite:* `{report.suite_name}`
*Time:* `{report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}`
*Rows:* `{report.row_count}`

*Results:* {report.passed_count}/{len(report.results)} passed
*Status:* {status}
"""
            await self.send_message(chat_id, message)
        
        elif command == "/help":
            await self.send_message(chat_id, """
*Available Commands:*
/check - Run data quality validation
/help - Show this help
""")
        
        else:
            await self.send_message(chat_id, "Unknown command. Send /help for commands.")
    
    async def run(self):
        """Main bot loop."""
        logger.info(f"Starting Telegram bot @{self.config.telegram.bot_name}")
        logger.info("Send /check to run validation")
        
        self.running = True
        
        while self.running:
            try:
                updates = await self.get_updates()
                
                for update in updates:
                    self.offset = update["update_id"] + 1
                    
                    if "message" in update:
                        msg = update["message"]
                        chat_id = msg["chat"]["id"]
                        user_name = msg.get("from", {}).get("first_name", "User")
                        text = msg.get("text", "")
                        
                        if text.startswith("/"):
                            command = text.split()[0].split("@")[0]
                            await self.handle_command(chat_id, command, user_name)
                        else:
                            await self.send_message(chat_id, "Send /check to run validation")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Bot error: {e}")
                await asyncio.sleep(5)
        
        logger.info("Bot stopped")


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Data Quality Validation Service")
    parser.add_argument("--bot", action="store_true", help="Start Telegram bot")
    
    args = parser.parse_args()
    
    if args.bot:
        try:
            import aiohttp
        except ImportError:
            print("Please install aiohttp: pip install aiohttp")
            sys.exit(1)
        
        bot = TelegramBot()
        print("=" * 60)
        print("Data Quality Telegram Bot")
        print("=" * 60)
        print(f"Bot: @{bot.config.telegram.bot_name}")
        print("Commands: /start, /check, /help")
        print("=" * 60)
        
        asyncio.run(bot.run())
    else:
        service = ValidationService()
        report = asyncio.run(service.run_validation())
        
        if report.is_success:
            print("\n[SUCCESS] All data quality checks passed!")
        else:
            print(f"\n[FAILED] {report.failed_count} checks failed!")
            sys.exit(1)


if __name__ == "__main__":
    main()
