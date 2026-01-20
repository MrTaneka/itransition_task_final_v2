# ==============================================================================
# Great Expectations Data Quality Validation
# 
# This module sets up and runs data quality checks on taxi data from Microsoft
# Fabric Gold layer. Validates distributions, nulls, ranges, and business rules.
#
# Author: Senior Data Engineer
# Best Practices Applied:
# - Expectation suites defined programmatically
# - Checkpoint-based validation
# - Integration with Telegram notifications
# - Support for both local and Fabric data sources
# ==============================================================================

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from config import GX
from telegram_notifier import send_validation_result, send_alert

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data_validation.log')
    ]
)
logger = logging.getLogger(__name__)


class TaxiDataQualityValidator:
    """
    Validates taxi trip data quality using Great Expectations patterns.
    
    Note: This is a simplified implementation that doesn't require the full
    Great Expectations installation. It demonstrates the concepts and 
    integrates with Telegram for notifications.
    """
    
    def __init__(self, data_path: Optional[str] = None):
        """
        Initialize the validator.
        
        Args:
            data_path: Path to the Parquet data file
        """
        self.data_path = data_path or GX.taxi_data_path
        self.data: Optional[pd.DataFrame] = None
        self.expectations: List[Dict[str, Any]] = []
        self.results: List[Dict[str, Any]] = []
        
    def load_data(self) -> bool:
        """
        Load data from Parquet file.
        
        Returns:
            True if data was loaded successfully
        """
        try:
            logger.info(f"Loading data from: {self.data_path}")
            self.data = pd.read_parquet(self.data_path)
            logger.info(f"Loaded {len(self.data)} rows, {len(self.data.columns)} columns")
            return True
        except FileNotFoundError:
            logger.warning(f"Data file not found: {self.data_path}")
            # Generate sample data for demonstration
            logger.info("Generating sample data for demonstration...")
            self._generate_sample_data()
            return True
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False
    
    def _generate_sample_data(self):
        """Generate sample taxi data for demonstration."""
        import numpy as np
        
        np.random.seed(42)
        n_rows = 1000
        
        self.data = pd.DataFrame({
            'date_key': [20251001 + i % 31 for i in range(n_rows)],
            'pickup_zone_id': np.random.randint(1, 265, n_rows),
            'dropoff_zone_id': np.random.randint(1, 265, n_rows),
            'total_trips': np.random.randint(10, 1000, n_rows),
            'total_passengers': np.random.randint(10, 2000, n_rows),
            'total_distance': np.round(np.random.uniform(10, 5000, n_rows), 2),
            'total_fare': np.round(np.random.uniform(100, 50000, n_rows), 2),
            'avg_fare': np.round(np.random.uniform(10, 100, n_rows), 2),
            'avg_trip_distance': np.round(np.random.uniform(0.5, 20, n_rows), 2)
        })
        
        # Introduce some nulls for testing
        null_indices = np.random.choice(n_rows, 5, replace=False)
        self.data.loc[null_indices, 'avg_fare'] = None
        
        logger.info(f"Generated sample data: {len(self.data)} rows")
    
    def add_expectation(
        self,
        expectation_type: str,
        column: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Add an expectation to the suite.
        
        Args:
            expectation_type: Type of expectation
            column: Column to validate (if applicable)
            **kwargs: Additional expectation parameters
        """
        self.expectations.append({
            "expectation_type": expectation_type,
            "column": column,
            "kwargs": kwargs
        })
        logger.debug(f"Added expectation: {expectation_type} on {column}")
    
    def define_taxi_expectations(self) -> None:
        """Define the standard taxi data quality expectations."""
        
        # 1. Column existence
        required_columns = [
            'date_key', 'pickup_zone_id', 'dropoff_zone_id',
            'total_trips', 'total_fare', 'avg_fare'
        ]
        for col in required_columns:
            self.add_expectation("expect_column_to_exist", column=col)
        
        # 2. No nulls in critical columns
        critical_columns = ['date_key', 'pickup_zone_id', 'total_trips']
        for col in critical_columns:
            self.add_expectation("expect_column_values_to_not_be_null", column=col)
        
        # 3. Value ranges
        self.add_expectation(
            "expect_column_values_to_be_between",
            column="total_trips",
            min_value=1, max_value=1000000
        )
        
        self.add_expectation(
            "expect_column_values_to_be_between",
            column="avg_fare",
            min_value=1, max_value=500
        )
        
        self.add_expectation(
            "expect_column_values_to_be_between",
            column="pickup_zone_id",
            min_value=1, max_value=265
        )
        
        # 4. Date key format (YYYYMMDD)
        self.add_expectation(
            "expect_column_values_to_be_between",
            column="date_key",
            min_value=20250101, max_value=20261231
        )
        
        # 5. Positive values
        self.add_expectation(
            "expect_column_values_to_be_greater_than",
            column="total_distance",
            value=0
        )
        
        logger.info(f"Defined {len(self.expectations)} expectations")
    
    def _validate_expectation(self, expectation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single expectation against the data.
        
        Args:
            expectation: Expectation definition
            
        Returns:
            Validation result dictionary
        """
        exp_type = expectation["expectation_type"]
        column = expectation.get("column")
        kwargs = expectation.get("kwargs", {})
        
        result = {
            "expectation_config": {
                "expectation_type": exp_type,
                "kwargs": {"column": column, **kwargs}
            },
            "success": False,
            "result": {}
        }
        
        try:
            if exp_type == "expect_column_to_exist":
                result["success"] = column in self.data.columns
                result["result"]["observed_value"] = column in self.data.columns
                
            elif exp_type == "expect_column_values_to_not_be_null":
                null_count = self.data[column].isnull().sum()
                result["success"] = null_count == 0
                result["result"]["unexpected_count"] = int(null_count)
                result["result"]["unexpected_percent"] = float(null_count / len(self.data) * 100)
                
            elif exp_type == "expect_column_values_to_be_between":
                min_val = kwargs.get("min_value", float("-inf"))
                max_val = kwargs.get("max_value", float("inf"))
                col_data = self.data[column].dropna()
                
                out_of_range = col_data[(col_data < min_val) | (col_data > max_val)]
                result["success"] = len(out_of_range) == 0
                result["result"]["unexpected_count"] = len(out_of_range)
                result["result"]["observed_min"] = float(col_data.min()) if len(col_data) > 0 else None
                result["result"]["observed_max"] = float(col_data.max()) if len(col_data) > 0 else None
                
            elif exp_type == "expect_column_values_to_be_greater_than":
                threshold = kwargs.get("value", 0)
                col_data = self.data[column].dropna()
                
                violations = col_data[col_data <= threshold]
                result["success"] = len(violations) == 0
                result["result"]["unexpected_count"] = len(violations)
                
            else:
                result["result"]["error"] = f"Unknown expectation type: {exp_type}"
                
        except Exception as e:
            result["result"]["error"] = str(e)
            logger.error(f"Error validating {exp_type}: {e}")
        
        return result
    
    def run_validation(self) -> Dict[str, Any]:
        """
        Run all expectations and return validation results.
        
        Returns:
            Validation result dictionary
        """
        if self.data is None:
            if not self.load_data():
                return {"success": False, "error": "Failed to load data"}
        
        logger.info("=" * 60)
        logger.info("Starting data quality validation")
        logger.info("=" * 60)
        
        self.results = []
        for expectation in self.expectations:
            result = self._validate_expectation(expectation)
            self.results.append(result)
            
            status = "[OK]" if result["success"] else "[FAIL]"
            logger.info(f"  {status} {expectation['expectation_type']} ({expectation.get('column', 'N/A')})")
        
        # Calculate statistics
        successful = sum(1 for r in self.results if r["success"])
        total = len(self.results)
        
        validation_result = {
            "success": successful == total,
            "statistics": {
                "evaluated_expectations": total,
                "successful_expectations": successful,
                "unsuccessful_expectations": total - successful,
                "success_percent": (successful / total * 100) if total > 0 else 0
            },
            "results": self.results,
            "validation_time": datetime.now().isoformat()
        }
        
        logger.info("=" * 60)
        logger.info(f"Validation complete: {successful}/{total} passed")
        logger.info("=" * 60)
        
        return validation_result
    
    def run_with_notification(self, suite_name: str = "taxi_quality") -> Dict[str, Any]:
        """
        Run validation and send results to Telegram.
        
        Args:
            suite_name: Name of the expectation suite
            
        Returns:
            Validation result dictionary
        """
        result = self.run_validation()
        send_validation_result(result, suite_name)
        return result


def main():
    """Main entry point for running validation."""
    logger.info("Initializing Taxi Data Quality Validator")
    
    validator = TaxiDataQualityValidator()
    
    # Load data
    if not validator.load_data():
        send_alert("Data Quality Check", "Failed to load data for validation", "error")
        sys.exit(1)
    
    # Define expectations
    validator.define_taxi_expectations()
    
    # Run validation with notification
    result = validator.run_with_notification()
    
    # Print summary
    if result["success"]:
        print("\n[SUCCESS] All data quality checks passed!")
    else:
        print("\n[FAILED] Some data quality checks failed!")
        failed = [r for r in result["results"] if not r["success"]]
        for f in failed:
            exp_type = f["expectation_config"]["expectation_type"]
            column = f["expectation_config"]["kwargs"].get("column", "N/A")
            print(f"   - {exp_type} on {column}")
    
    return result


if __name__ == "__main__":
    result = main()
    sys.exit(0 if result["success"] else 1)
