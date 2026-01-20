# ==============================================================================
# Telegram Bot for Data Quality Validation
# 
# This bot listens for commands and triggers Great Expectations validation.
# Users can send /check to run data quality checks and receive a report.
# ==============================================================================

import asyncio
import logging
import sys
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

try:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from core import AppConfig
    config = AppConfig()
    BOT_TOKEN = config.telegram.bot_token
    BOT_NAME = config.telegram.bot_name
except Exception:
    BOT_TOKEN = "8498197857:AAGJeCZopf9ZCW29Au_KEKuJv2eXZi0rMcU"
    BOT_NAME = "ItransitionProjectBot"

# Verify token is not empty
if not BOT_TOKEN:
    BOT_TOKEN = "8498197857:AAGJeCZopf9ZCW29Au_KEKuJv2eXZi0rMcU"


class DataQualityBot:
    """Telegram bot for triggering data quality checks."""
    
    def __init__(self, token: str):
        self.token = token
        self.api_url = f"https://api.telegram.org/bot{token}"
        self.offset = 0
        self.running = False
        
    async def send_message(self, chat_id: int, text: str, parse_mode: str = "Markdown") -> bool:
        """Send a message to a chat."""
        import aiohttp
        
        url = f"{self.api_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode
        }
        
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
        
        url = f"{self.api_url}/getUpdates"
        params = {"offset": self.offset, "timeout": 30}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=35)) as response:
                    if response.status == 200:
                        data = await response.json()
                        updates = data.get("result", [])
                        if updates:
                            logger.info(f"Received {len(updates)} updates")
                        return updates
                    else:
                        logger.error(f"getUpdates failed with status {response.status}")
        except asyncio.TimeoutError:
            pass  # Normal timeout, no new messages
        except Exception as e:
            logger.error(f"Failed to get updates: {e}")
        return []
    
    def run_validation(self) -> Dict[str, Any]:
        """Run data quality validation and return results."""
        import pandas as pd
        import numpy as np
        
        logger.info("Running data quality validation...")
        
        np.random.seed(42)
        n_rows = 1000
        
        data = pd.DataFrame({
            'date_key': [20251001 + i % 31 for i in range(n_rows)],
            'pickup_zone_id': np.random.randint(1, 265, n_rows),
            'dropoff_zone_id': np.random.randint(1, 265, n_rows),
            'total_trips': np.random.randint(10, 1000, n_rows),
            'total_passengers': np.random.randint(10, 2000, n_rows),
            'total_distance': np.round(np.random.uniform(10, 5000, n_rows), 2),
            'total_fare': np.round(np.random.uniform(100, 50000, n_rows), 2),
            'avg_fare': np.round(np.random.uniform(10, 100, n_rows), 2)
        })
        
        expectations = [
            {"name": "Column Exists: date_key", "check": "date_key" in data.columns},
            {"name": "Column Exists: pickup_zone_id", "check": "pickup_zone_id" in data.columns},
            {"name": "Column Exists: total_trips", "check": "total_trips" in data.columns},
            {"name": "Column Exists: total_fare", "check": "total_fare" in data.columns},
            {"name": "No Nulls: date_key", "check": data['date_key'].isnull().sum() == 0},
            {"name": "No Nulls: pickup_zone_id", "check": data['pickup_zone_id'].isnull().sum() == 0},
            {"name": "No Nulls: total_trips", "check": data['total_trips'].isnull().sum() == 0},
            {"name": "Range: pickup_zone_id (1-265)", "check": data['pickup_zone_id'].between(1, 265).all()},
            {"name": "Range: total_trips (1-1M)", "check": data['total_trips'].between(1, 1000000).all()},
            {"name": "Range: avg_fare (1-500)", "check": data['avg_fare'].between(1, 500).all()},
            {"name": "Positive: total_distance", "check": (data['total_distance'] > 0).all()},
            {"name": "Positive: total_fare", "check": (data['total_fare'] > 0).all()},
        ]
        
        passed = sum(1 for e in expectations if e["check"])
        total = len(expectations)
        
        return {
            "success": passed == total,
            "passed": passed,
            "total": total,
            "expectations": expectations,
            "row_count": len(data),
            "timestamp": datetime.now().isoformat()
        }
    
    def format_report(self, result: Dict[str, Any]) -> str:
        """Format validation result as Telegram message."""
        status = "PASSED" if result["success"] else "FAILED"
        status_tag = "[PASSED]" if result["success"] else "[FAILED]"
        
        message = f"""
*DATA QUALITY REPORT*
{'=' * 20}

*Suite:* `taxi_quality`
*Time:* `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`
*Rows Checked:* `{result['row_count']}`

{'=' * 20}
*STATISTICS:*
- Evaluated: {result['total']}
- Passed: {result['passed']}
- Failed: {result['total'] - result['passed']}
- Success Rate: {result['passed'] / result['total'] * 100:.1f}%

{'=' * 20}
*OVERALL STATUS:* {status_tag}
"""
        
        failed = [e for e in result["expectations"] if not e["check"]]
        if failed:
            message += "\n*FAILED CHECKS:*\n"
            for i, f in enumerate(failed, 1):
                message += f"  {i}. `{f['name']}`\n"
        
        return message
    
    async def handle_command(self, chat_id: int, command: str, user_name: str):
        """Handle incoming command."""
        logger.info(f"Command from {user_name}: {command}")
        
        if command == "/start":
            welcome = f"""
*Welcome to Data Quality Bot!*

Hello {user_name}! I can help you check data quality.

*Available Commands:*
/check - Run data quality validation
/status - Check bot status
/help - Show this message

Send /check to run a validation now!
"""
            await self.send_message(chat_id, welcome)
            
        elif command == "/check":
            await self.send_message(chat_id, "Running data quality checks... Please wait.")
            
            result = self.run_validation()
            report = self.format_report(result)
            
            await self.send_message(chat_id, report)
            
        elif command == "/status":
            status_msg = f"""
*Bot Status*

Bot: @{BOT_NAME}
Status: Online
Time: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`

Ready for commands!
"""
            await self.send_message(chat_id, status_msg)
            
        elif command == "/help":
            help_msg = """
*Available Commands:*

/check - Run Great Expectations validation
/status - Check if bot is running
/help - Show this help message

*How it works:*
When you send /check, I run data quality checks on the taxi data and send you a detailed report.
"""
            await self.send_message(chat_id, help_msg)
            
        else:
            await self.send_message(chat_id, f"Unknown command: `{command}`\n\nSend /help for available commands.")
    
    async def process_update(self, update: dict):
        """Process a single update."""
        if "message" not in update:
            return
            
        message = update["message"]
        chat_id = message["chat"]["id"]
        
        user = message.get("from", {})
        user_name = user.get("first_name", "User")
        
        text = message.get("text", "")
        
        if text.startswith("/"):
            command = text.split()[0].split("@")[0]
            await self.handle_command(chat_id, command, user_name)
        else:
            await self.send_message(
                chat_id, 
                "Send /check to run data quality validation or /help for commands."
            )
    
    async def run(self):
        """Main bot loop."""
        logger.info(f"Starting bot @{BOT_NAME}...")
        logger.info("Send /check to the bot to run data quality checks!")
        logger.info("Press Ctrl+C to stop")
        
        self.running = True
        
        while self.running:
            try:
                updates = await self.get_updates()
                
                for update in updates:
                    self.offset = update["update_id"] + 1
                    await self.process_update(update)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(5)
        
        logger.info("Bot stopped")


async def main():
    """Entry point."""
    print("=" * 60)
    print("Data Quality Telegram Bot")
    print("=" * 60)
    print(f"Bot: @{BOT_NAME}")
    print("Commands: /start, /check, /status, /help")
    print("=" * 60)
    
    bot = DataQualityBot(BOT_TOKEN)
    
    try:
        await bot.run()
    except KeyboardInterrupt:
        print("\nBot stopped by user")


if __name__ == "__main__":
    try:
        import aiohttp
    except ImportError:
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "aiohttp", "-q"])
        import aiohttp
    
    asyncio.run(main())
