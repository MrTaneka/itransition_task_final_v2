# ==============================================================================
# Telegram Notification Service
# 
# Sends data quality validation results to Telegram.
# ==============================================================================

import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional

import requests

from config import TELEGRAM

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Send notifications to Telegram."""
    
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        """
        Initialize the Telegram notifier.
        
        Args:
            bot_token: Telegram bot token (from @BotFather)
            chat_id: Telegram chat ID to send messages to
        """
        self.bot_token = bot_token or TELEGRAM.bot_token
        self.chat_id = chat_id or TELEGRAM.chat_id
        self.api_url = TELEGRAM.api_url
        
    def _is_configured(self) -> bool:
        """Check if Telegram is properly configured."""
        return (
            self.bot_token != "YOUR_BOT_TOKEN_HERE" and 
            self.chat_id != "YOUR_CHAT_ID_HERE"
        )
    
    def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        """
        Send a message to Telegram.
        
        Args:
            text: Message text (supports Markdown)
            parse_mode: Parse mode (Markdown or HTML)
            
        Returns:
            True if message was sent successfully
        """
        if not self._is_configured():
            logger.warning("Telegram not configured. Message not sent.")
            print(f"\n[TELEGRAM PREVIEW]\n{text}\n")
            return False
        
        url = f"{self.api_url}/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info("Telegram message sent successfully")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    def send_validation_report(
        self, 
        validation_result: Dict[str, Any],
        suite_name: str = "taxi_quality"
    ) -> bool:
        """
        Send a formatted validation report to Telegram.
        
        Args:
            validation_result: Great Expectations validation result
            suite_name: Name of the expectation suite
            
        Returns:
            True if message was sent successfully
        """
        success = validation_result.get("success", False)
        statistics = validation_result.get("statistics", {})
        # Determine status
        status_text = "[PASSED]" if success else "[FAILED]"
        
        # Format the message
        message = f"""
*DATA QUALITY REPORT*
{'=' * 20}

*Suite:* `{suite_name}`
*Time:* `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`

{'=' * 20}
*STATISTICS:*
- Evaluated: {statistics.get('evaluated_expectations', 0)}
- Successful: {statistics.get('successful_expectations', 0)}
- Failed: {statistics.get('unsuccessful_expectations', 0)}
- Success Rate: {statistics.get('success_percent', 0):.1f}%

{'=' * 20}
*OVERALL STATUS:* {status_text}
"""
        
        # Add failed expectations if any
        if not success and "results" in validation_result:
            failed = [r for r in validation_result["results"] if not r.get("success", True)]
            if failed:
                message += "\n*FAILED CHECKS:*\n"
                for i, f in enumerate(failed[:5], 1):  # Limit to 5
                    exp_type = f.get("expectation_config", {}).get("expectation_type", "unknown")
                    message += f"  {i}. `{exp_type}`\n"
        
        return self.send_message(message)
    
    def send_alert(self, title: str, message: str, level: str = "info") -> bool:
        """
        Send a simple alert message.
        
        Args:
            title: Alert title
            message: Alert message
            level: Alert level (info, warning, error)
            
        Returns:
            True if message was sent successfully
        """
        level_map = {
            "info": "[INFO]",
            "warning": "[WARNING]",
            "error": "[ERROR]"
        }
        
        level_tag = level_map.get(level, "[INFO]")
        
        formatted_message = f"""
*ALERT: {title}*
{'=' * 20}

{level_tag} {message}

*Time:* `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`
"""
        return self.send_message(formatted_message)


notifier = TelegramNotifier()


def send_validation_result(validation_result: Dict[str, Any], suite_name: str = "taxi_quality") -> bool:
    """Convenience function to send validation results."""
    return notifier.send_validation_report(validation_result, suite_name)


def send_alert(title: str, message: str, level: str = "info") -> bool:
    """Convenience function to send alerts."""
    return notifier.send_alert(title, message, level)
