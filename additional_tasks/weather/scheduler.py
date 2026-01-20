# ==============================================================================
# Scheduled Weather Ingestion Job
# 
# This module runs the weather ingestion on a schedule using APScheduler.
# Designed for production deployment with proper lifecycle management.
# ==============================================================================

import atexit
import signal
import sys
import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import SCHEDULER
from weather_ingestion import run_ingestion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create scheduler
scheduler = BlockingScheduler()


def scheduled_job():
    """Execute the weather ingestion as a scheduled job."""
    logger.info("Scheduled job triggered")
    try:
        result = run_ingestion()
        if result["status"] != "success":
            logger.error(f"Scheduled job failed: {result.get('errors', [])}")
    except Exception as e:
        logger.error(f"Unexpected error in scheduled job: {e}")


def shutdown_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Shutdown signal received, stopping scheduler...")
    scheduler.shutdown(wait=False)
    sys.exit(0)


def main():
    """Main entry point for the scheduled service."""
    logger.info("=" * 60)
    logger.info("Weather Ingestion Scheduler Starting")
    logger.info(f"Interval: {SCHEDULER.weather_interval_minutes} minutes")
    logger.info("=" * 60)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    # Register cleanup on exit
    atexit.register(lambda: scheduler.shutdown(wait=False))
    
    # Run immediately on startup
    logger.info("Running initial ingestion...")
    scheduled_job()
    
    # Schedule periodic runs
    scheduler.add_job(
        scheduled_job,
        trigger=IntervalTrigger(minutes=SCHEDULER.weather_interval_minutes),
        id='weather_ingestion',
        name='Weather Data Ingestion',
        replace_existing=True,
        next_run_time=datetime.now()  # Also run immediately
    )
    
    logger.info(f"Scheduler started. Next run in {SCHEDULER.weather_interval_minutes} minutes.")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
