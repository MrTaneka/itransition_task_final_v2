# ==============================================================================
# Power Automate JSON Export Service
#
# Generates JSON reports from Gold layer data and uploads to OneDrive for
# triggering Power Automate flows.
#
# Architecture:
#   - Service Pattern: ExportService orchestrates the pipeline
#   - Builder Pattern: ReportBuilder constructs JSON reports
#   - Repository Pattern: OneDriveRepository handles file uploads
#
# Usage:
#   python -m power_automate.service              # Export once
#   python -m power_automate.service --period 2025-10  # Specify period
#
# Author: Senior Data Engineer
# ==============================================================================

import argparse
import json
import shutil
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import AppConfig, DataSourceError, LoggerFactory, ensure_directory

logger = LoggerFactory.get_logger(__name__)


# ==============================================================================
# DATA MODELS
# ==============================================================================

@dataclass
class TopRecord:
    """Top performing record for email preview."""
    date: str
    zone_id: int
    trips: int
    revenue: str
    avg_fare: str


@dataclass
class ExportReport:
    """
    Complete export report structure.
    
    Flat structure optimized for Power Automate Parse JSON action.
    """
    # Metadata
    report_title: str
    period: str
    export_date: str
    export_time: str
    row_count: int
    source: str = "Microsoft Fabric Gold Layer"
    
    # Summary statistics
    total_trips: int = 0
    total_revenue: str = "$0.00"
    total_passengers: int = 0
    avg_fare: str = "$0.00"
    avg_distance: str = "0.0 miles"
    
    # Top record
    top_zone_date: str = "N/A"
    top_zone_trips: int = 0
    top_zone_revenue: str = "$0.00"
    
    # Notification
    notification_title: str = ""
    notification_message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


# ==============================================================================
# BUILDER PATTERN - Report Construction
# ==============================================================================

class ReportBuilder:
    """
    Builder for constructing ExportReport from data.
    
    Uses Builder Pattern for step-by-step report construction.
    """
    
    def __init__(self, period: str):
        self.period = period
        self.now = datetime.now()
        self._df: Optional[pd.DataFrame] = None
        self._report: Optional[ExportReport] = None
    
    def with_data(self, df: pd.DataFrame) -> 'ReportBuilder':
        """Set the source dataframe."""
        self._df = df
        return self
    
    def build(self) -> ExportReport:
        """Build the final report."""
        if self._df is None or len(self._df) == 0:
            raise DataSourceError("No data available for report")
        
        df = self._df
        
        # Calculate summary statistics
        total_trips = int(df['total_trips'].sum())
        total_passengers = int(df['total_passengers'].sum()) if 'total_passengers' in df.columns else 0
        total_revenue_value = float(df['total_fare'].sum())
        avg_fare_value = float(df['avg_fare'].mean())
        avg_distance_value = float(df['avg_trip_distance'].mean()) if 'avg_trip_distance' in df.columns else 0
        
        # Get top record
        if 'total_fare' in df.columns:
            top_row = df.loc[df['total_fare'].idxmax()]
            top_zone_date = str(top_row.get('date_key', 'N/A'))
            top_zone_trips = int(top_row.get('total_trips', 0))
            top_zone_revenue = f"${float(top_row.get('total_fare', 0)):,.2f}"
        else:
            top_zone_date = "N/A"
            top_zone_trips = 0
            top_zone_revenue = "$0.00"
        
        return ExportReport(
            report_title=f"Taxi Data Report - {self.period}",
            period=self.period,
            export_date=self.now.strftime("%Y-%m-%d"),
            export_time=self.now.strftime("%H:%M:%S"),
            row_count=len(df),
            
            total_trips=total_trips,
            total_revenue=f"${total_revenue_value:,.2f}",
            total_passengers=total_passengers,
            avg_fare=f"${avg_fare_value:.2f}",
            avg_distance=f"{avg_distance_value:.1f} miles",
            
            top_zone_date=top_zone_date,
            top_zone_trips=top_zone_trips,
            top_zone_revenue=top_zone_revenue,
            
            notification_title=f"New Fabric Report: {self.period}",
            notification_message=f"Taxi data export completed. {len(df)} records processed. Total revenue: ${total_revenue_value:,.2f}"
        )


# ==============================================================================
# REPOSITORY PATTERN - File Storage
# ==============================================================================

class LocalFileRepository:
    """Repository for local file operations."""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        ensure_directory(str(self.base_path))
    
    def save_json(self, filename: str, content: str) -> Path:
        """Save JSON content to file."""
        filepath = self.base_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"Saved JSON to {filepath}")
        return filepath


class OneDriveRepository:
    """
    Repository for OneDrive file operations.
    
    Uses local sync folder approach. For production, would use 
    Microsoft Graph API for direct upload.
    """
    
    def __init__(self, sync_path: str):
        self.sync_path = Path(sync_path)
    
    @property
    def is_available(self) -> bool:
        return self.sync_path.exists() or self._create_folder()
    
    def _create_folder(self) -> bool:
        """Attempt to create the sync folder."""
        try:
            self.sync_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.warning(f"Could not create OneDrive folder: {e}")
            return False
    
    def upload(self, source_path: Path) -> Optional[Path]:
        """
        Upload file to OneDrive sync folder.
        
        In production, this would use Microsoft Graph API.
        For demo, we copy to the local OneDrive sync folder.
        """
        if not self.is_available:
            logger.warning("OneDrive folder not available")
            return None
        
        try:
            dest_path = self.sync_path / source_path.name
            shutil.copy2(source_path, dest_path)
            logger.info(f"Copied to OneDrive: {dest_path}")
            return dest_path
        except Exception as e:
            logger.error(f"Failed to copy to OneDrive: {e}")
            return None


# ==============================================================================
# DATA LOADER
# ==============================================================================

class DataLoader:
    """
    Loads taxi data from various sources.
    
    Supports:
    - Microsoft Fabric OneLake (when capacity is running)
    - Local parquet files
    - Sample data generation (fallback)
    """
    
    # OneLake paths for Fabric workspace
    FABRIC_WORKSPACE = "itransitionproject"
    FABRIC_LAKEHOUSE = "lakehouse_nyc_taxi"
    
    def __init__(self, config: Optional[AppConfig] = None):
        self.config = config or AppConfig()
    
    def load_from_fabric(self) -> Optional[pd.DataFrame]:
        """
        Load data directly from Microsoft Fabric OneLake.
        
        OneLake path format:
        abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/{table}
        
        Note: Requires Fabric capacity to be running and proper authentication.
        """
        # OneLake paths (when accessed from Fabric notebook or with proper auth)
        onelake_paths = [
            # Direct OneLake path
            f"abfss://{self.FABRIC_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{self.FABRIC_LAKEHOUSE}/Tables/FactTaxiDaily",
            # Alternative: Files path
            f"abfss://{self.FABRIC_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{self.FABRIC_LAKEHOUSE}/Files/gold/FactTaxiDaily.parquet",
        ]
        
        for path in onelake_paths:
            try:
                logger.info(f"Attempting to load from Fabric: {path}")
                df = pd.read_parquet(path)
                logger.info(f"Successfully loaded {len(df)} rows from Fabric OneLake")
                return df
            except Exception as e:
                logger.debug(f"Could not load from {path}: {e}")
                continue
        
        return None
    
    def load_gold_layer(self) -> Optional[pd.DataFrame]:
        """Load from local Gold layer parquet file."""
        path = self.config.data.gold_layer_path
        if Path(path).exists():
            logger.info(f"Loading from local Gold layer: {path}")
            return pd.read_parquet(path)
        return None
    
    def load_or_generate(self, source: str = "auto") -> pd.DataFrame:
        """
        Load data from specified source or auto-detect.
        
        Args:
            source: Data source - 'fabric', 'local', 'sample', or 'auto'
        
        Returns:
            DataFrame with taxi data
        """
        if source == "fabric":
            df = self.load_from_fabric()
            if df is not None:
                return df
            raise DataSourceError("Fabric data not available. Is capacity running?")
        
        if source == "local":
            df = self.load_gold_layer()
            if df is not None:
                return df
            raise DataSourceError(f"Local file not found: {self.config.data.gold_layer_path}")
        
        if source == "sample":
            logger.info("Generating sample data")
            return self._generate_sample()
        
        # Auto mode: try Fabric -> local -> sample
        if source == "auto":
            # Try Fabric first (when capacity is running)
            df = self.load_from_fabric()
            if df is not None:
                return df
            
            # Try local file
            df = self.load_gold_layer()
            if df is not None:
                return df
            
            # Fall back to sample
            logger.info("No data source available, generating sample data")
            return self._generate_sample()
        
        raise ValueError(f"Unknown source: {source}. Use 'fabric', 'local', 'sample', or 'auto'")
    
    def _generate_sample(self) -> pd.DataFrame:
        """Generate sample taxi data matching Gold layer schema."""
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


# ==============================================================================
# SERVICE PATTERN - Export Orchestration
# ==============================================================================

class ExportService:
    """
    Main service for JSON export pipeline.
    
    Orchestrates data loading, report building, and file upload.
    """
    
    def __init__(
        self,
        data_loader: Optional[DataLoader] = None,
        local_repo: Optional[LocalFileRepository] = None,
        onedrive_repo: Optional[OneDriveRepository] = None,
        config: Optional[AppConfig] = None
    ):
        self.config = config or AppConfig()
        self.data_loader = data_loader or DataLoader(self.config)
        self.local_repo = local_repo or LocalFileRepository(self.config.data.export_output_dir)
        self.onedrive_repo = onedrive_repo or OneDriveRepository(self.config.data.onedrive_sync_path)
    
    def run_export(self, period: str = "2025-10") -> Dict[str, Any]:
        """
        Execute the complete export pipeline.
        
        Returns:
            Dict with export results including file paths
        """
        logger.info("=" * 60)
        logger.info("Power Automate JSON Export")
        logger.info("=" * 60)
        
        result = {
            "success": False,
            "local_path": None,
            "onedrive_path": None,
            "error": None
        }
        
        try:
            # Load data
            df = self.data_loader.load_or_generate()
            logger.info(f"Loaded {len(df)} records")
            
            # Build report
            report = ReportBuilder(period).with_data(df).build()
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"taxi_report_{period}_{timestamp}.json"
            
            # Save locally
            local_path = self.local_repo.save_json(filename, report.to_json())
            result["local_path"] = str(local_path)
            
            # Upload to OneDrive
            onedrive_path = self.onedrive_repo.upload(local_path)
            if onedrive_path:
                result["onedrive_path"] = str(onedrive_path)
            
            result["success"] = True
            
            logger.info("=" * 60)
            logger.info(f"Export complete: {filename}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            result["error"] = str(e)
        
        return result


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================

def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Power Automate JSON Export Service")
    parser.add_argument(
        "--period",
        type=str,
        default="2025-10",
        help="Report period (default: 2025-10)"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["auto", "fabric", "local", "sample"],
        default="auto",
        help="Data source: auto (try all), fabric (OneLake), local (parquet file), sample (generated)"
    )
    
    args = parser.parse_args()
    
    service = ExportService()
    
    # Custom load with source selection
    data_loader = service.data_loader
    df = data_loader.load_or_generate(source=args.source)
    
    result = {
        "success": False,
        "local_path": None,
        "onedrive_path": None,
        "source": args.source
    }
    
    try:
        logger.info("=" * 60)
        logger.info("Power Automate JSON Export")
        logger.info(f"Data source: {args.source}")
        logger.info("=" * 60)
        
        logger.info(f"Loaded {len(df)} records")
        
        # Build report
        report = ReportBuilder(args.period).with_data(df).build()
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"taxi_report_{args.period}_{timestamp}.json"
        
        # Save locally
        local_path = service.local_repo.save_json(filename, report.to_json())
        result["local_path"] = str(local_path)
        
        # Upload to OneDrive
        onedrive_path = service.onedrive_repo.upload(local_path)
        if onedrive_path:
            result["onedrive_path"] = str(onedrive_path)
        
        result["success"] = True
        
        logger.info("=" * 60)
        logger.info(f"Export complete: {filename}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        result["error"] = str(e)
    
    if result["success"]:
        print(f"\n[SUCCESS] Export successful!")
        print(f"   Source: {args.source}")
        print(f"   Local: {result['local_path']}")
        if result.get("onedrive_path"):
            print(f"   OneDrive: {result['onedrive_path']}")
            print("\n[INFO] Power Automate flow should trigger now!")
        else:
            print("\n[WARNING] OneDrive upload failed - manual upload required")
    else:
        print(f"\n[FAILED] Export failed: {result.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
