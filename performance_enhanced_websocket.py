import os
import time
import json
import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

logger = logging.getLogger(__name__)

class PerformanceEnhancedWebSocketClient:
    """Enhanced WebSocket client with comprehensive progress tracking"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.symbols = []
        self.active_websockets = {}
        self.klines = {}
        self.running = False
        self.websocket_threads = []
        
        logger.info("Performance Enhanced WebSocket client initialized")
    
    def load_historical_data(self, symbol, interval='1h', limit=100):
        """Load historical kline data from Binance API"""
        try:
            url = f"https://api.binance.com/api/v3/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            klines_data = response.json()
            processed_klines = []
            
            for kline in klines_data:
                processed_klines.append({
                    'timestamp': int(kline[0]),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                })
            
            return processed_klines
            
        except Exception as e:
            logger.error(f"Error loading historical data for {symbol}: {e}")
            return []
    
    def _init_database(self):
        """Initialize database tables for kline data"""
        try:
            self.db_manager.execute_query('market_data', '''
            CREATE TABLE IF NOT EXISTS kline_data (
                symbol TEXT,
                timeframe TEXT,
                open_time INTEGER,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume REAL,
                close_time INTEGER,
                timestamp INTEGER,
                PRIMARY KEY (symbol, timeframe, open_time)
            )
            ''')
            logger.info("Kline data table initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing kline database: {e}")
    
    def _get_active_symbols(self):
        """Fetch active USDT trading pairs from Binance with fallback"""
        try:
            url = "https://api.binance.com/api/v3/ticker/24hr"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            tickers = response.json()
            usdt_symbols = []
            
            for ticker in tickers:
                symbol = ticker['symbol']
                if symbol.endswith('USDT'):
                    try:
                        price = float(ticker['lastPrice'])
                        if price >= 0.5:
                            usdt_symbols.append(symbol)
                        else:
                            logger.info(f"Excluded {symbol} due to price below 0.5 USDT")
                    except (ValueError, KeyError):
                        continue
            
            logger.info(f"Fetched {len(usdt_symbols)} active USDT trading pairs")
            return usdt_symbols
            
        except Exception as e:
            logger.warning(f"Error fetching active symbols from API: {e}")
            logger.info("Using fallback symbol list for testing...")
            
            fallback_symbols = [
                'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
                'SOLUSDT', 'DOTUSDT', 'DOGEUSDT', 'AVAXUSDT', 'MATICUSDT',
                'LTCUSDT', 'LINKUSDT', 'UNIUSDT', 'ATOMUSDT', 'ETCUSDT',
                'XLMUSDT', 'VETUSDT', 'FILUSDT', 'TRXUSDT', 'EOSUSDT',
                'AAVEUSDT', 'MKRUSDT', 'COMPUSDT', 'YFIUSDT', 'SUSHIUSDT',
                'SNXUSDT', 'CRVUSDT', 'BALUSDT', '1INCHUSDT', 'ENJUSDT',
                'MANAUSDT', 'SANDUSDT', 'CHZUSDT', 'GALAUSDT', 'AXSUSDT'
            ]
            
            logger.info(f"Using {len(fallback_symbols)} fallback USDT trading pairs")
            return fallback_symbols
    
    def get_active_symbols(self):
        """Get cached active symbols"""
        return self.symbols
    
    def initialize_market_data_enhanced(self):
        """Enhanced market data initialization with comprehensive progress tracking"""
        self.db_manager.initialize_schema()
        self._init_database()
        
        self.symbols = self._get_active_symbols()
        
        if not self.symbols:
            logger.error("No symbols available for initialization")
            return
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time as time_module
        
        total_symbols = len(self.symbols)
        processed_count = 0
        successful_count = 0
        failed_count = 0
        start_time = time_module.time()
        
        logger.info(f"🚀 Starting enhanced historical data initialization for {total_symbols} symbols")
        logger.info(f"📊 Progress: 0/{total_symbols} (0.00%) - ETA: Calculating...")
        
        def process_symbol_enhanced(symbol):
            """Enhanced symbol processing with dual table storage"""
            try:
                klines = self.load_historical_data(symbol, interval='1h', limit=100)
                
                if not klines:
                    return False, f"No data for {symbol}"
                
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
                    
                    self.db_manager.execute_query(
                        'market_data',
                        '''
                        INSERT OR REPLACE INTO kline_data
                        (symbol, timeframe, open_time, open_price, high_price, low_price, close_price, volume, close_time, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        params=(
                            symbol,
                            '1h',
                            kline['timestamp'],
                            kline['open'],
                            kline['high'],
                            kline['low'],
                            kline['close'],
                            kline['volume'],
                            kline['timestamp'] + 3600000,
                            kline['timestamp']
                        )
                    )
                
                return True, f"✅ Loaded {len(klines)} candles for {symbol}"
                
            except Exception as e:
                return False, f"❌ Failed to load {symbol}: {str(e)}"
        
        batch_size = 20
        max_workers = 12
        
        logger.info(f"🔧 Using {max_workers} workers with batch size {batch_size}")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, total_symbols, batch_size):
                batch = self.symbols[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (total_symbols + batch_size - 1) // batch_size
                
                logger.info(f"🔄 Processing batch {batch_num}/{total_batches}: {batch[:3]}{'...' if len(batch) > 3 else ''}")
                
                futures = {executor.submit(process_symbol_enhanced, symbol): symbol for symbol in batch}
                
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        success, message = future.result()
                        processed_count += 1
                        
                        if success:
                            successful_count += 1
                            logger.debug(message)
                        else:
                            failed_count += 1
                            logger.warning(message)
                        
                        elapsed_time = time_module.time() - start_time
                        progress_percent = (processed_count / total_symbols) * 100
                        
                        if processed_count > 0:
                            avg_time_per_symbol = elapsed_time / processed_count
                            remaining_symbols = total_symbols - processed_count
                            eta_seconds = remaining_symbols * avg_time_per_symbol
                            eta_minutes = eta_seconds / 60
                            
                            symbols_per_minute = processed_count / (elapsed_time / 60) if elapsed_time > 0 else 0
                            
                            logger.info(f"📊 Progress: {processed_count}/{total_symbols} ({progress_percent:.1f}%) | "
                                      f"✅ Success: {successful_count} | ❌ Failed: {failed_count} | "
                                      f"⚡ Rate: {symbols_per_minute:.1f}/min | ETA: {eta_minutes:.1f}min")
                        
                    except Exception as e:
                        processed_count += 1
                        failed_count += 1
                        logger.error(f"❌ Exception processing {symbol}: {e}")
                
                if i + batch_size < total_symbols:
                    time_module.sleep(0.2)
        
        total_time = time_module.time() - start_time
        success_rate = (successful_count / total_symbols) * 100 if total_symbols > 0 else 0
        
        logger.info(f"🎉 Historical data initialization completed!")
        logger.info(f"📈 Results: {successful_count}/{total_symbols} successful ({success_rate:.1f}%)")
        logger.info(f"⏱️ Total time: {total_time:.1f}s | Average: {total_time/total_symbols:.2f}s per symbol")
        logger.info(f"🚀 Bot is now ready to analyze market data and generate trading signals!")
        
        return successful_count > 0
    
    def start(self):
        """Start the enhanced WebSocket client"""
        if self.running:
            return
        
        self.running = True
        logger.info("Starting enhanced WebSocket client")
        
        success = self.initialize_market_data_enhanced()
        
        if success:
            logger.info("✅ Enhanced WebSocket client started successfully")
        else:
            logger.error("❌ Failed to initialize market data")
            self.running = False
    
    def stop(self):
        """Stop the WebSocket client"""
        self.running = False
        logger.info("Enhanced WebSocket client stopped")
    
    def is_running(self):
        """Check if client is running"""
        return self.running
    
    def get_historical_klines(self, symbol, timeframe):
        """Get cached historical klines"""
        return self.klines.get(f"{symbol}_{timeframe}", [])
    
    def get_current_price(self, symbol):
        """Get current price for symbol"""
        try:
            url = f"https://api.binance.com/api/v3/ticker/price"
            params = {'symbol': symbol}
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
            return float(data['price'])
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            return None
