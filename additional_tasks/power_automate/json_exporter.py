# ==============================================================================
# Power Automate JSON Export Service
# 
# This module exports taxi data from Microsoft Fabric Gold layer to JSON format
# for use with Power Automate. The JSON files are uploaded to OneDrive/Dropbox
# to trigger the Power Automate flow.
#
# Flow: Export JSON → Upload to OneDrive → Power Automate triggers → 
#       → Email notification → Mobile push notification
#
# Author: Senior Data Engineer
# ==============================================================================

import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PowerAutomateExporter:
    """
    Export data to JSON format for Power Automate integration.
    
    The exported JSON contains:
    - Export metadata (timestamp, period, row count)
    - Summary statistics 
    - Sample data rows for email preview
    """
    
    def __init__(
        self,
        data_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        onedrive_path: Optional[str] = None
    ):
        """
        Initialize the exporter.
        
        Args:
            data_path: Path to source Parquet data
            output_dir: Directory to save JSON exports
            onedrive_path: Path to OneDrive sync folder for auto-upload
        """
        self.data_path = data_path or "d:/itransition_task_final/data/gold/FactTaxiDaily.parquet"
        self.output_dir = Path(output_dir or "d:/itransition_task_final/additional_tasks/power_automate/exports")
        
        # Common OneDrive paths on Windows
        self.onedrive_path = onedrive_path or self._detect_onedrive_path()
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _detect_onedrive_path(self) -> Optional[str]:
        """Detect OneDrive folder path on Windows."""
        possible_paths = [
            os.path.expanduser("~/OneDrive"),
            os.path.expanduser("~/OneDrive - Personal"),
            "C:/Users/Taneka/OneDrive",
            "C:/Users/Taneka/OneDrive/FabricReports"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Detected OneDrive path: {path}")
                return path
        
        logger.warning("OneDrive path not detected")
        return None
    
    def load_data(self) -> Optional[pd.DataFrame]:
        """
        Load data from Parquet file or generate sample data.
        
        Returns:
            DataFrame with taxi data
        """
        try:
            if os.path.exists(self.data_path):
                logger.info(f"Loading data from: {self.data_path}")
                return pd.read_parquet(self.data_path)
        except Exception as e:
            logger.warning(f"Could not load data: {e}")
        
        # Generate sample data
        logger.info("Generating sample data for demonstration")
        return self._generate_sample_data()
    
    def _generate_sample_data(self) -> pd.DataFrame:
        """Generate sample taxi data."""
        import numpy as np
        np.random.seed(42)
        
        n_rows = 100
        return pd.DataFrame({
            'date_key': [20251001 + i % 31 for i in range(n_rows)],
            'pickup_zone_id': np.random.randint(1, 265, n_rows),
            'dropoff_zone_id': np.random.randint(1, 265, n_rows),
            'total_trips': np.random.randint(100, 5000, n_rows),
            'total_passengers': np.random.randint(100, 10000, n_rows),
            'total_fare': np.round(np.random.uniform(1000, 100000, n_rows), 2),
            'avg_fare': np.round(np.random.uniform(15, 50, n_rows), 2),
            'avg_trip_distance': np.round(np.random.uniform(1, 15, n_rows), 2)
        })
    
    def calculate_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate summary statistics from the data.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        return {
            "total_trips": int(df['total_trips'].sum()),
            "total_revenue_usd": round(float(df['total_fare'].sum()), 2),
            "total_passengers": int(df['total_passengers'].sum()),
            "avg_fare_usd": round(float(df['avg_fare'].mean()), 2),
            "avg_trip_distance_miles": round(float(df['avg_trip_distance'].mean()), 2),
            "unique_pickup_zones": int(df['pickup_zone_id'].nunique()),
            "unique_dropoff_zones": int(df['dropoff_zone_id'].nunique()),
            "min_date_key": int(df['date_key'].min()),
            "max_date_key": int(df['date_key'].max())
        }
    
    def format_for_email(self, df: pd.DataFrame, n_rows: int = 5) -> List[Dict[str, Any]]:
        """
        Format top rows for email display.
        
        Args:
            df: Source DataFrame
            n_rows: Number of rows to include
            
        Returns:
            List of row dictionaries
        """
        # Sort by total_fare descending to show top revenue records
        top_rows = df.nlargest(n_rows, 'total_fare')
        
        formatted = []
        for _, row in top_rows.iterrows():
            formatted.append({
                "date": str(row['date_key']),
                "pickup_zone": int(row['pickup_zone_id']),
                "trips": int(row['total_trips']),
                "revenue": f"${row['total_fare']:,.2f}",
                "avg_fare": f"${row['avg_fare']:.2f}"
            })
        
        return formatted
    
    def export_to_json(
        self,
        df: Optional[pd.DataFrame] = None,
        period: str = "2025-10"
    ) -> str:
        """
        Export data to JSON file.
        
        Args:
            df: DataFrame to export (loads from path if None)
            period: Reporting period identifier
            
        Returns:
            Path to the exported JSON file
        """
        if df is None:
            df = self.load_data()
        
        if df is None or len(df) == 0:
            raise ValueError("No data available for export")
        
        # Calculate summary
        summary = self.calculate_summary(df)
        top_records = self.format_for_email(df, n_rows=3)
        
        # Build FLAT export document for Power Automate compatibility
        # All fields at root level for easy Dynamic content access
        export_doc = {
            # Metadata fields
            "report_title": f"Taxi Data Report - {period}",
            "period": period,
            "export_date": datetime.now().strftime("%Y-%m-%d"),
            "export_time": datetime.now().strftime("%H:%M:%S"),
            "row_count": len(df),
            "source": "Microsoft Fabric Gold Layer",
            
            # Summary statistics (flat)
            "total_trips": summary["total_trips"],
            "total_revenue": f"${summary['total_revenue_usd']:,.2f}",
            "total_passengers": summary["total_passengers"],
            "avg_fare": f"${summary['avg_fare_usd']:.2f}",
            "avg_distance": f"{summary['avg_trip_distance_miles']:.1f} miles",
            
            # Top record for email preview
            "top_zone_date": top_records[0]["date"] if top_records else "N/A",
            "top_zone_trips": top_records[0]["trips"] if top_records else 0,
            "top_zone_revenue": top_records[0]["revenue"] if top_records else "$0",
            
            # Notification message
            "notification_title": f"New Fabric Report: {period}",
            "notification_message": f"Taxi data export completed. {len(df)} records processed. Total revenue: ${summary['total_revenue_usd']:,.2f}"
        }
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"taxi_report_{period}_{timestamp}.json"
        filepath = self.output_dir / filename
        
        # Write JSON file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_doc, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported JSON to: {filepath}")
        return str(filepath)
    
    def copy_to_onedrive(self, json_path: str) -> Optional[str]:
        """
        Copy JSON file to OneDrive for Power Automate trigger.
        
        Args:
            json_path: Path to the JSON file
            
        Returns:
            Path to the OneDrive copy, or None if failed
        """
        if not self.onedrive_path:
            logger.warning("OneDrive path not configured")
            return None
        
        try:
            # Create FabricReports folder if needed
            onedrive_folder = Path(self.onedrive_path) / "FabricReports"
            onedrive_folder.mkdir(parents=True, exist_ok=True)
            
            # Copy file
            dest_path = onedrive_folder / Path(json_path).name
            shutil.copy2(json_path, dest_path)
            
            logger.info(f"Copied to OneDrive: {dest_path}")
            return str(dest_path)
            
        except Exception as e:
            logger.error(f"Failed to copy to OneDrive: {e}")
            return None
    
    def run_export(self, period: str = "2025-10") -> Dict[str, Any]:
        """
        Run the complete export workflow.
        
        Args:
            period: Reporting period
            
        Returns:
            Export result dictionary
        """
        result = {
            "success": False,
            "json_path": None,
            "onedrive_path": None,
            "error": None
        }
        
        try:
            # Export to JSON
            json_path = self.export_to_json(period=period)
            result["json_path"] = json_path
            
            # Copy to OneDrive
            onedrive_path = self.copy_to_onedrive(json_path)
            result["onedrive_path"] = onedrive_path
            
            result["success"] = True
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            result["error"] = str(e)
        
        return result


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Power Automate JSON Export")
    logger.info("=" * 60)
    
    exporter = PowerAutomateExporter()
    result = exporter.run_export(period="2025-10")
    
    if result["success"]:
        print("\n[SUCCESS] Export successful!")
        print(f"   JSON file: {result['json_path']}")
        if result["onedrive_path"]:
            print(f"   OneDrive: {result['onedrive_path']}")
            print("\n[INFO] Power Automate flow should trigger now!")
        else:
            print("\n[WARNING] OneDrive not configured - manual upload required")
    else:
        print(f"\n[FAILED] Export failed: {result['error']}")
    
    return result


if __name__ == "__main__":
    main()
