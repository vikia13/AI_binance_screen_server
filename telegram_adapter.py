import requests
import logging
import threading
import time
import json
import sqlite3
from typing import Dict, Any, Optional
import os

logger = logging.getLogger(__name__)

class EnhancedTelegramBot:
    def __init__(self, token, chat_id=None, signal_generator=None, indicator_manager=None, websocket_client=None):
        self.token = token
        self.chat_id = chat_id
        self.signal_generator = signal_generator
        self.indicator_manager = indicator_manager
        self.websocket_client = websocket_client
        self.base_url = f"https://api.telegram.org/bot{token}/"
        self.start_time = time.time()
        self.running = False
        self.update_thread = None
        self.last_update_id = 0
        self.command_handlers = {
            'start': self._handle_start_command,
            'help': self._handle_help_command,
            'status': self._handle_status_command,
            'symbols': self._handle_symbols_command
        }

        # Test API connection
        try:
            response = requests.get(f"{self.base_url}getMe", timeout=5)
            if response.status_code == 200:
                logger.info("Telegram bot initialized successfully")
            else:
                raise Exception(f"API Error: {response.text}")
        except Exception as e:
            logger.error(f"Failed to initialize Telegram bot: {e}")
            raise

    def start(self):
        """Start the bot - send startup message and begin polling for updates"""
        try:
            # Send initial message
            self.send_message("AI Trading Bot is now online.")

            # Start polling in a separate thread
            self.running = True
            self.update_thread = threading.Thread(target=self._polling_loop)
            self.update_thread.daemon = True
            self.update_thread.start()

            logger.info("Telegram bot started successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to start Telegram bot: {e}")
            return False

    def _polling_loop(self):
        """Poll for updates using HTTP API"""
        while self.running:
            try:
                updates = self._get_updates()
                if updates:
                    self._process_updates(updates)
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(5)

    def _get_updates(self):
        """Get updates from Telegram API"""
        try:
            params = {'timeout': 30, 'offset': self.last_update_id + 1}
            response = requests.get(f"{self.base_url}getUpdates", params=params, timeout=31)
            if response.ok:
                data = response.json()
                if data.get('result'):
                    # Update the last_update_id
                    self.last_update_id = max(update['update_id'] for update in data['result'])
                    return data['result']
            return []
        except Exception as e:
            logger.error(f"Error getting updates: {e}")
            return []

    def _process_updates(self, updates):
        """Process received updates"""
        for update in updates:
            try:
                # Handle messages with commands
                if 'message' in update and 'text' in update['message']:
                    message = update['message']
                    chat_id = message['chat']['id']
                    text = message['text']

                    if text.startswith('/'):
                        command = text[1:].split()[0].lower()
                        self._handle_command(command, chat_id, message)
            except Exception as e:
                logger.error(f"Error processing update: {e}")

    def _handle_command(self, command, chat_id, message):
        """Handle received command"""
        if command in self.command_handlers:
            try:
                self.command_handlers[command](chat_id, message)
                logger.info(f"Handled command: {command}")
            except Exception as e:
                logger.error(f"Error handling command {command}: {e}")
                self.send_message("Error processing command. Please try again.", chat_id)
        else:
            self.send_message("Unknown command. Type /help for available commands.", chat_id)

    def _handle_start_command(self, chat_id, message):
        """Handle /start command"""
        user = message.get('from', {}).get('username') or message.get('from', {}).get('first_name', 'User')
        logger.info(f"Received /start command from {user}")

        welcome_text = (
            "Welcome to the AI Trading System Bot!\n\n"
            "Use /help to see available commands."
        )
        self.send_message(welcome_text, chat_id)

    def _handle_help_command(self, chat_id, message):
        """Handle /help command"""
        user = message.get('from', {}).get('username') or message.get('from', {}).get('first_name', 'User')
        logger.info(f"Received /help command from {user}")

        help_text = (
            "Available commands:\n"
            "/start - Start the bot\n"
            "/help - Show this help message\n"
            "/status - Show system status\n"
            "/symbols - List monitored symbols"
        )
        self.send_message(help_text, chat_id)

    def _handle_status_command(self, chat_id, message):
        """Handle /status command"""
        user = message.get('from', {}).get('username') or message.get('from', {}).get('first_name', 'User')
        logger.info(f"Received /status command from {user}")

        # Get system status information
        active_symbols = 0
        if self.websocket_client and hasattr(self.websocket_client, 'get_active_symbols'):
            active_symbols = len(self.websocket_client.get_active_symbols())

        status_text = (
            "🤖 AI Trading System Status:\n\n"
            f"Active symbols: {active_symbols}\n"
            f"System uptime: {self._get_uptime()}"
        )
        self.send_message(status_text, chat_id)

    def _handle_symbols_command(self, chat_id, message):
        """Handle /symbols command"""
        user = message.get('from', {}).get('username') or message.get('from', {}).get('first_name', 'User')
        logger.info(f"Received /symbols command from {user}")

        symbols = []
        if self.websocket_client and hasattr(self.websocket_client, 'get_active_symbols'):
            symbols = self.websocket_client.get_active_symbols()

        if symbols:
            symbols_text = "Currently monitoring:\n" + "\n".join(symbols[:20])
            if len(symbols) > 20:
                symbols_text += f"\n...and {len(symbols) - 20} more"
        else:
            symbols_text = "Not monitoring any symbols currently."

        self.send_message(symbols_text, chat_id)

    def _get_uptime(self):
        """Get system uptime"""
        uptime_seconds = int(time.time() - self.start_time)
        hours, remainder = divmod(uptime_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

    def send_message(self, message, chat_id=None, parse_mode='Markdown'):
        """Send message using HTTP API"""
        if not chat_id:
            chat_id = self.chat_id

        if not chat_id:
            logger.error("Cannot send message: chat_id not set")
            return False

        try:
            params = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': parse_mode
            }
            response = requests.post(f"{self.base_url}sendMessage", json=params, timeout=10)
            return response.ok
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def send_signal(self, signal):
        """Send trading signal notification"""
        direction_emoji = "🟢" if signal['direction'] == "LONG" else "🔴"
        message = f"🚨 *NEW TRADING SIGNAL*\n\n"
        message += f"{signal['symbol']} {direction_emoji} {signal['direction']}\n"
        message += f"Entry Price: ${signal['entry_price']:.4f}\n"

        if 'take_profit' in signal and signal['take_profit']:
            message += f"Take Profit: ${signal['take_profit']:.4f}\n"

        if 'stop_loss' in signal and signal['stop_loss']:
            message += f"Stop Loss: ${signal['stop_loss']:.4f}\n"

        if 'confidence' in signal:
            message += f"Confidence: {signal['confidence']:.1f}%\n"

        if 'model_name' in signal:
            message += f"Model: {signal['model_name']}\n"

        return self.send_message(message)

    def stop(self):
        """Stop the bot"""
        self.running = False

        if self.update_thread and self.update_thread.is_alive():
            self.update_thread.join(timeout=1.0)

        logger.info("Telegram bot stopped")
        return True