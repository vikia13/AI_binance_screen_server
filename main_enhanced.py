import os
import logging
import time
import threading
import signal
import sys
import queue
from telegram.ext._utils.webhookhandler import TelegramHandler
from database_manager import DatabaseManager
from websocket_client import BinanceWebSocketClient
from indicators import TechnicalIndicators
from ai_model_enhanced import EnhancedAIModel
from signal_generator import SignalGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class TradingBot:
    """Main trading bot application with enhanced components"""

    def __init__(self, db_path='data', telegram_token=None, allowed_users=None):
        self.db_path = db_path
        self.running = False
        self.components = {}
        self.telegram_token = telegram_token
        self.allowed_users = allowed_users or []
        self.signal_queue = queue.Queue()

        # Create data directory if it doesn't exist
        os.makedirs(db_path, exist_ok=True)

        # Initialize components
        self._init_components()

        logger.info("Trading bot initialized with enhanced components")

    def _init_components(self):
        """Initialize all trading bot components"""
        try:
            # Initialize database manager with db_path
            self.components['database'] = DatabaseManager(self.db_path)
            self.db_manager = self.components['database']

            # Initialize components with the db_manager object
            self.components['websocket'] = BinanceWebSocketClient(self.db_manager)
            self.components['indicators'] = TechnicalIndicators(self.db_manager)
            self.components['ai_model'] = EnhancedAIModel(self.db_manager)
            self.components['signals'] = SignalGenerator(
                self.db_manager,
                self.components['indicators'],
                self.components['ai_model']
            )

            # Add Telegram notification handler if token is valid
            if self.telegram_token and len(self.telegram_token) > 20:
                try:
                    from telegram_adapter import TelegramAdapter
                    self.components['telegram'] = TelegramAdapter(
                        token=self.telegram_token,
                        allowed_users=self.allowed_users,
                        components=self.components
                    )
                    logger.info(
                        f"Telegram bot initialized with token: {self.telegram_token[:5]}...{self.telegram_token[-5:]}")
                except ImportError as e:
                    logger.error(f"Could not import telegram_adapter: {e}")
                    logger.warning("Telegram notifications will be disabled")
                except Exception as e:
                    logger.error(f"Error initializing Telegram component: {e}")
                    logger.warning("Telegram notifications will be disabled")
            else:
                logger.warning("No valid Telegram token provided, Telegram notifications will be disabled")

        except Exception as e:
            logger.error(f"Error initializing components: {e}")
            raise

    def start(self):
        """Start the trading bot and all its components"""
        if self.running:
            logger.warning("Trading bot is already running")
            return

        self.running = True
        logger.info("Starting trading bot")

        try:
            # Start WebSocket client to collect market data
            self.components['websocket'].start()

            # Allow WebSocket connections to stabilize
            time.sleep(5)

            # Start signal processing in a separate thread
            self.signal_thread = threading.Thread(target=self._process_signals)
            self.signal_thread.daemon = True
            self.signal_thread.start()

            # Start telegram notification thread
            self.telegram_thread = threading.Thread(target=self._process_telegram_queue)
            self.telegram_thread.daemon = True
            self.telegram_thread.start()

            # Start Telegram bot if available
            if 'telegram' in self.components:
                try:
                    logger.info("Starting Telegram bot")
                    self.components['telegram'].start()
                    logger.info("Telegram bot started successfully")
                except Exception as e:
                    logger.error(f"Error starting Telegram bot: {e}", exc_info=True)
                    # Remove failed telegram component
                    del self.components['telegram']

            logger.info("Trading bot started successfully")
        except Exception as e:
            self.running = False
            logger.error(f"Error starting trading bot: {e}", exc_info=True)
            raise

    def stop(self):
        """Stop the trading bot and all its components"""
        if not self.running:
            return

        self.running = False
        logger.info("Stopping trading bot")

        try:
            # Stop Telegram bot if available
            if 'telegram' in self.components:
                try:
                    logger.info("Stopping Telegram bot")
                    self.components['telegram'].stop()
                    logger.info("Telegram bot stopped successfully")
                except Exception as e:
                    logger.error(f"Error stopping Telegram bot: {e}")

            # Stop WebSocket client
            if 'websocket' in self.components:
                self.components['websocket'].stop()

            # Wait for threads to finish
            if hasattr(self, 'signal_thread') and self.signal_thread.is_alive():
                self.signal_thread.join(timeout=5)

            if hasattr(self, 'telegram_thread') and self.telegram_thread.is_alive():
                self.telegram_thread.join(timeout=5)

            # Close all database connections
            if 'database' in self.components:
                self.components['database'].close_all()

            logger.info("Trading bot stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping trading bot: {e}", exc_info=True)

    def _process_telegram_queue(self):
        """Process signals in the queue and send to Telegram"""
        while self.running:
            try:
                # Get signals from queue with timeout
                try:
                    signal = self.signal_queue.get(timeout=5)
                    if 'telegram' in self.components:
                        try:
                            logger.info(f"Sending signal to Telegram: {signal}")
                            self.components['telegram'].send_signal_notification(signal)
                        except Exception as e:
                            logger.error(f"Error sending signal to Telegram: {e}")
                    self.signal_queue.task_done()
                except queue.Empty:
                    pass  # No signals in queue, continue
            except Exception as e:
                logger.error(f"Error in Telegram queue processing: {e}")
                time.sleep(5)  # Prevent tight loop on error

    def _process_signals(self):
        """Process market data and generate signals"""
        timeframes = ["1m", "5m", "15m", "1h", "4h"]

        # Check if required components are available
        if 'indicators' not in self.components or 'signals' not in self.components:
            logger.error("Required components missing, cannot process signals")
            return

        # Initial delay to allow WebSocket connections to establish
        time.sleep(30)

        check_interval = 60  # Check every minute instead of 5 minutes
        symbols_per_batch = 5  # Process fewer symbols per batch

        while self.running:
            try:
                # Get active symbols from WebSocket client
                symbols = self.components['websocket'].get_active_symbols()

                if not symbols:
                    logger.warning("No active symbols available")
                    time.sleep(30)
                    continue

                # Process each symbol in batches
                for i in range(0, min(20, len(symbols)), symbols_per_batch):
                    batch_symbols = symbols[i:i+symbols_per_batch]
                    logger.info(f"Processing symbols batch: {batch_symbols}")

                    # Process each symbol in the batch
                    for symbol in batch_symbols:
                        # Get klines data for each timeframe
                        klines_data = {}
                        for timeframe in timeframes:
                            klines = self.components['websocket'].get_historical_klines(symbol, timeframe)
                            if klines:
                                klines_data[timeframe] = klines

                        if not klines_data:
                            logger.warning(f"No klines data available for {symbol}")
                            continue

                        # Calculate technical indicators
                        try:
                            indicators_result = self.components['indicators'].calculate_indicators(symbol, "1h")
                            if not indicators_result:
                                logger.warning(f"Failed to calculate indicators for {symbol}")
                                continue
                        except Exception as e:
                            logger.error(f"Error calculating indicators for {symbol}: {e}")
                            continue

                        # Generate signals
                        try:
                            signals = self.components['signals'].generate_signals(symbol, timeframes, klines_data)

                            if signals:
                                logger.info(f"Generated {len(signals)} signals for {symbol}")

                                # Queue signals for Telegram
                                for signal in signals:
                                    self.signal_queue.put(signal)
                        except Exception as e:
                            logger.error(f"Error generating signals for {symbol}: {e}")

                    # Small delay between batches to prevent overload
                    time.sleep(5)

                # Sleep to avoid excessive CPU usage
                time.sleep(check_interval)

            except Exception as e:
                logger.error(f"Error in signal processing: {e}", exc_info=True)
                time.sleep(30)  # Wait and retry

    def run_forever(self):
        """Run the bot until interrupted"""
        self.start()

        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            logger.info("Shutdown signal received")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("Trading bot running. Press Ctrl+C to stop.")

        # Log active components
        logger.info(f"Active components: {list(self.components.keys())}")

        # Periodically check and report status
        status_interval = 300  # Every 5 minutes
        last_status_time = time.time()

        # Keep the main thread alive
        while self.running:
            # Periodically log status
            current_time = time.time()
            if current_time - last_status_time > status_interval:
                if 'telegram' in self.components:
                    telegram_status = "Running" if hasattr(self.components['telegram'], 'is_running') and self.components['telegram'].is_running else "Not running"
                    logger.info(f"Telegram bot status: {telegram_status}")

                websocket_status = "Running" if self.components['websocket'].is_running() else "Not running"
                logger.info(f"WebSocket client status: {websocket_status}")

                # Log queued signals count
                logger.info(f"Signals in queue: {self.signal_queue.qsize()}")
                last_status_time = current_time

            time.sleep(1)


if __name__ == "__main__":
    # Set your Telegram bot token and allowed user IDs here
    TELEGRAM_TOKEN = "7633147170:AAGTGmkeVSnCdBnO8d5Pzxx7v7WzNBfhSNI"  # Your valid token

    ALLOWED_USERS = [5829074137]  # Your user ID

    logger.info(f"Allowed Telegram users: {ALLOWED_USERS}")

    bot = TradingBot(telegram_token=TELEGRAM_TOKEN, allowed_users=ALLOWED_USERS)
    bot.run_forever()