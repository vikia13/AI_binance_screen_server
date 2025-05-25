import json
import logging
import threading
import time
import websocket
import requests
from datetime import datetime
import queue
import pandas as pd

from httpx import Client

logger = logging.getLogger(__name__)

class BinanceWebSocketClient:
    """Client for Binance WebSocket API"""

    def __init__(self, db_manager, base_url="wss://stream.binance.com:9443/ws/"):
        """Initialize WebSocket client with database manager"""
        self.db_manager = db_manager
        self.base_url = base_url
        self.active_websockets = {}
        self.prices = {}
        self.klines = {}
        self.websocket_lock = threading.Lock()
        self.price_lock = threading.Lock()
        self.klines_lock = threading.Lock()
        self.running = False
        self.connection_threads = []
        self.rest_api_url = "https://api.binance.com/api/v3"
        self.client = Client()  # Initialize the client first

        # Create database tables
        self._init_database()

        # Get symbols before initialize_market_data
        self.symbols = self._get_active_symbols()

        # Now call initialize_market_data after symbols are defined
        self.initialize_market_data()

        logger.info("Binance WebSocket client initialized")

    def load_historical_data(self, symbol, interval='1h', limit=100):
        """
        Load historical kline data from Binance API
        """
        try:
            logger.info(f"Loading historical data for {symbol} at {interval} timeframe")

            # Use the REST API to get klines
            response = requests.get(
                f"{self.rest_api_url}/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "limit": limit
                }
            )

            if response.status_code != 200:
                logger.error(f"Failed to fetch klines for {symbol}: {response.text}")
                return []

            klines = response.json()

            # Process kline data
            data = []
            for kline in klines:
                data.append({
                    'timestamp': kline[0],
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                })

            logger.info(f"Loaded {len(data)} historical candles for {symbol}")
            return data

        except Exception as e:
            logger.error(f"Failed to load historical data for {symbol}: {e}")
            return []
    def _init_database(self):
        """Initialize database tables for market data"""
        conn = self.db_manager.get_connection('kline_data')
        cursor = conn.cursor()

        # Create klines table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS klines (
                symbol TEXT,
                timeframe TEXT,
                open_time INTEGER,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume REAL,
                close_time INTEGER,
                PRIMARY KEY (symbol, timeframe, open_time)
            )
        ''')

        # Create current prices table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS current_prices (
                symbol TEXT PRIMARY KEY,
                price REAL,
                updated_at INTEGER
            )
        ''')

        conn.commit()
        logger.info("Database initialized for market data")

    def start(self):
        """Start WebSocket client and connect to active symbols"""
        if self.running:
            logger.warning("WebSocket client already running")
            return

        self.running = True

        # Get list of active USDT trading pairs
        try:
            symbols = self._get_active_symbols()

            # Connect to first 10 symbols only (to reduce connection issues)
            # You can adjust this number based on your needs
            max_connections = 10
            for symbol in symbols[:max_connections]:
                self._connect_websocket(symbol.lower(), "1m")
                # Add delay between connections to prevent overload
                time.sleep(0.5)

            logger.info(f"Connected to {min(max_connections, len(symbols))} symbols")
        except Exception as e:
            logger.error(f"Error starting WebSocket client: {e}", exc_info=True)

    def stop(self):
        """Stop WebSocket client and close all connections"""
        if not self.running:
            return

        self.running = False

        with self.websocket_lock:
            for symbol, ws in list(self.active_websockets.items()):
                try:
                    if ws and hasattr(ws, 'close'):
                        ws.close()
                except Exception as e:
                    logger.error(f"Error closing WebSocket for {symbol}: {e}")

        # Wait for all connection threads to finish
        for thread in self.connection_threads:
            if thread.is_alive():
                thread.join(timeout=2)

        self.active_websockets.clear()
        self.connection_threads.clear()

        logger.info("WebSocket client stopped")

    def _get_active_symbols(self):
        """Get list of active USDT trading pairs from Binance"""
        try:
            response = requests.get(f"{self.rest_api_url}/exchangeInfo")
            data = response.json()

            symbols = []
            for symbol_data in data['symbols']:
                if symbol_data['status'] == 'TRADING' and symbol_data['quoteAsset'] == 'USDT':
                    symbol = symbol_data['symbol']
                    price = self.get_current_price(symbol)

                    # Skip low-priced assets (often very volatile and less interesting)
                    if price and price < 0.5:
                        logger.info(f"Excluded {symbol} due to price below 0.5 USDT")
                        continue

                    symbols.append(symbol)

            logger.info(f"Fetched {len(symbols)} active USDT trading pairs")
            return symbols
        except Exception as e:
            logger.error(f"Error fetching active symbols: {e}", exc_info=True)
            return []

    def get_active_symbols(self):
        """Get list of currently active symbols"""
        with self.websocket_lock:
            return [symbol.upper() for symbol in self.active_websockets.keys()]

    def _connect_websocket(self, symbol, interval):
        """Connect to Binance WebSocket for a specific symbol and interval"""
        stream_name = f"{symbol}@kline_{interval}"

        # Create WebSocket connection
        ws = None

        def on_message(ws, message):
            try:
                data = json.loads(message)

                # Process kline data
                if 'k' in data:
                    kline = data['k']

                    # Update prices
                    with self.price_lock:
                        self.prices[symbol.upper()] = float(kline['c'])

                    # Only save completed klines
                    if kline['x']:
                        self._save_kline(symbol.upper(), interval, kline)

                        # Update klines cache
                        with self.klines_lock:
                            if symbol.upper() not in self.klines:
                                self.klines[symbol.upper()] = {}
                            if interval not in self.klines[symbol.upper()]:
                                self.klines[symbol.upper()][interval] = []

                            # Add new kline to cache
                            new_kline = {
                                'open_time': kline['t'],
                                'open': float(kline['o']),
                                'high': float(kline['h']),
                                'low': float(kline['l']),
                                'close': float(kline['c']),
                                'volume': float(kline['v']),
                                'close_time': kline['T']
                            }

                            # Keep only last 100 klines
                            self.klines[symbol.upper()][interval].append(new_kline)
                            if len(self.klines[symbol.upper()][interval]) > 100:
                                self.klines[symbol.upper()][interval].pop(0)
            except Exception as e:
                logger.error(f"Error processing WebSocket message: {e}", exc_info=True)

        def on_error(ws, error):
            logger.error(f"WebSocket error ({stream_name}): {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.info(f"WebSocket closed ({stream_name}): {close_msg}")

            # Attempt reconnection if still running
            if self.running:
                # Remove from active websockets
                with self.websocket_lock:
                    if symbol in self.active_websockets:
                        self.active_websockets.pop(symbol, None)

                # Wait before reconnecting
                time.sleep(5)
                logger.warning(f"Connection lost for {stream_name}, reconnecting...")
                self._connect_websocket(symbol, interval)

        def on_open(ws):
            logger.info(f"WebSocket connected: {stream_name}")

        def run_websocket():
            """Run WebSocket connection in a loop with reconnection logic"""
            ws_url = f"{self.base_url}{stream_name}"

            while self.running:
                try:
                    # Create a new WebSocket instance each time
                    ws = websocket.WebSocketApp(
                        ws_url,
                        on_open=on_open,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close
                    )

                    # Add to active websockets
                    with self.websocket_lock:
                        self.active_websockets[symbol] = ws

                    # Run WebSocket (blocking)
                    ws.run_forever(ping_interval=30, ping_timeout=10, ping_payload="ping")

                    # If we're here, the connection was closed
                    if not self.running:
                        break

                    # Wait before reconnecting
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"Error in WebSocket connection for {stream_name}: {e}", exc_info=True)
                    time.sleep(5)  # Wait before retry

        # Start WebSocket in a new thread
        thread = threading.Thread(target=run_websocket)
        thread.daemon = True
        thread.start()

        # Keep track of connection threads
        self.connection_threads.append(thread)

    def _save_kline(self, symbol, interval, kline):
        """Save kline data to database"""
        try:
            conn = self.db_manager.get_connection('kline_data')
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO klines
                (symbol, timeframe, open_time, open_price, high_price, low_price, close_price, volume, close_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                interval,
                kline['t'],
                float(kline['o']),
                float(kline['h']),
                float(kline['l']),
                float(kline['c']),
                float(kline['v']),
                kline['T']
            ))

            conn.commit()
        except Exception as e:
            logger.error(f"Error saving kline data: {e}", exc_info=True)

    def get_current_price(self, symbol):
        """Get current price for a symbol"""
        try:
            # First check if we have it in memory
            with self.price_lock:
                if symbol in self.prices:
                    return self.prices[symbol]

            # If not, get from API
            response = requests.get(f"{self.rest_api_url}/ticker/price", params={"symbol": symbol})
            data = response.json()

            if 'price' in data:
                price = float(data['price'])

                # Update cache
                with self.price_lock:
                    self.prices[symbol] = price

                return price
            return None
        except Exception as e:
            logger.error(f"Error getting current price: {e}")
            return None

    def get_historical_klines(self, symbol, interval, limit=100):
        """Get historical klines from memory cache or database"""
        try:
            # First check if we have it in memory
            with self.klines_lock:
                if symbol in self.klines and interval in self.klines[symbol]:
                    return self.klines[symbol][interval]

            # If not in memory, get from database
            conn = self.db_manager.get_connection('kline_data')
            cursor = conn.cursor()

            cursor.execute('''
                SELECT open_time, open_price, high_price, low_price, close_price, volume, close_time
                FROM klines
                WHERE symbol = ? AND timeframe = ?
                ORDER BY open_time DESC
                LIMIT ?
            ''', (symbol, interval, limit))

            rows = cursor.fetchall()

            if not rows:
                return None

            klines = []
            for row in rows:
                klines.append({
                    'open_time': row[0],
                    'open': row[1],
                    'high': row[2],
                    'low': row[3],
                    'close': row[4],
                    'volume': row[5],
                    'close_time': row[6]
                })

            # Update memory cache
            with self.klines_lock:
                if symbol not in self.klines:
                    self.klines[symbol] = {}
                self.klines[symbol][interval] = klines

            return klines
        except Exception as e:
            logger.error(f"Error getting historical klines: {e}", exc_info=True)
            return None

    def initialize_market_data(self):
        """Fetch initial historical data for all symbols"""
        # Ensure the market_data table exists with the right schema
        self.db_manager.initialize_schema()

        # Use a ThreadPool to download data in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def process_symbol(symbol):
            try:
                # Use the load_historical_data method
                klines = self.load_historical_data(symbol, interval='1h', limit=100)

                # Process and store the klines
                for kline in klines:
                    self.db_manager.execute_query(
                        'market_data',
                        '''
                        INSERT OR IGNORE INTO market_data
                        (symbol, timestamp, timeframe, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        params=(
                            symbol,
                            kline['timestamp'],
                            '1h',
                            kline['open'],
                            kline['high'],
                            kline['low'],
                            kline['close'],
                            kline['volume']
                        )
                    )

                if klines:
                    logger.info(f"Loaded initial historical data for {symbol}")
                return True
            except Exception as e:
                logger.error(f"Failed to load historical data for {symbol}: {e}")
                return False

        # Process symbols in batches of 10 with max 5 concurrent workers
        batch_size = 10
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in range(0, len(self.symbols), batch_size):
                batch = self.symbols[i:i + batch_size]
                futures = {executor.submit(process_symbol, symbol): symbol for symbol in batch}

                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Exception processing {symbol}: {e}")

                # Small delay between batches to prevent API rate limits
                if i + batch_size < len(self.symbols):
                    time.sleep(1)
    def is_running(self):
        """Check if WebSocket client is running"""
        return self.running