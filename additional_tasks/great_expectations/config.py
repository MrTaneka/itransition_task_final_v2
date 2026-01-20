# ==============================================================================
# Great Expectations Data Quality Configuration
# 
# Configuration for data validation using Great Expectations.
# Validates taxi trip data from Microsoft Fabric Gold layer.
# ==============================================================================

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class GXConfig:
    """Configuration for Great Expectations."""
    
    # Local data paths (for testing without Fabric connection)
    local_data_path: str = os.getenv(
        "LOCAL_DATA_PATH",
        "d:/itransition_task_final/data/gold"
    )
    
    # Sample data for validation
    taxi_data_path: str = os.getenv(
        "TAXI_DATA_PATH",
        "d:/itransition_task_final/data/gold/FactTaxiDaily.parquet"
    )
    
    # Great Expectations project directory
    gx_directory: str = os.getenv(
        "GX_DIRECTORY",
        "d:/itransition_task_final/additional_tasks/great_expectations/gx"
    )


@dataclass(frozen=True)
class TelegramConfig:
    """Configuration for Telegram bot notifications."""
    
    # Get from @BotFather on Telegram
    bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "8498197857:AAGJeCZopf9ZCW29Au_KEKuJv2eXZi0rMcU")
    
    # Get from @userinfobot on Telegram
    chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # API endpoint
    api_url: str = "https://api.telegram.org"


# Global instances
GX = GXConfig()
TELEGRAM = TelegramConfig()
