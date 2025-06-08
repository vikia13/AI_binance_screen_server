import os
import logging
import time
import threading
import signal
import sys
import queue
from enhanced_database_manager import EnhancedDatabaseManager
from performance_enhanced_websocket import PerformanceEnhancedWebSocketClient
from indicators import TechnicalIndicators
from ai_model_enhanced import EnhancedAIModel
from signal_generator import SignalGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("enhanced_trading_bot.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class EnhancedTradingBot:
    """Enhanced trading bot with comprehensive performance improvements"""
    
    def __init__(self, db_path='data', telegram_token=None, allowed_users=None):
        self.db_path = db_path
        self.running = False
        self.components = {}
        self.telegram_token = telegram_token
        self.allowed_users = allowed_users or []
        self.signal_queue = queue.Queue()
        self.initialization_complete = False
        
        os.makedirs(db_path, exist_ok=True)
        self._init_enhanced_components()
        
        logger.info("🚀 Enhanced trading bot initialized with performance improvements")
    
    def _init_enhanced_components(self):
        """Initialize enhanced components with performance optimizations"""
        try:
            logger.info("🔧 Initializing enhanced components...")
            
            self.components['database'] = EnhancedDatabaseManager(self.db_path)
            self.db_manager = self.components['database']
            
            self.components['websocket'] = PerformanceEnhancedWebSocketClient(self.db_manager)
            self.components['indicators'] = TechnicalIndicators(self.db_manager)
            self.components['ai_model'] = EnhancedAIModel(self.db_manager)
            self.components['signals'] = SignalGenerator(
                self.db_manager,
                self.components['indicators'],
                self.components['ai_model']
            )
            
            if self.telegram_token and len(self.telegram_token) > 20:
                try:
                    from telegram_adapter import TelegramAdapter
                    self.components['telegram'] = TelegramAdapter(
                        token=self.telegram_token,
                        allowed_users=self.allowed_users,
                        components=self.components
                    )
                    logger.info(f"🤖 Telegram bot initialized with token: {self.telegram_token[:5]}...{self.telegram_token[-5:]}")
                except Exception as e:
                    logger.error(f"❌ Error initializing Telegram: {e}")
            
            logger.info("✅ Enhanced components initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Error initializing enhanced components: {e}")
            raise
    
    def start(self):
        """Start enhanced trading bot with performance monitoring"""
        if self.running:
            logger.warning("⚠️ Enhanced trading bot is already running")
            return
        
        self.running = True
        logger.info("🚀 Starting enhanced trading bot with performance improvements")
        
        try:
            logger.info("📊 Starting market data initialization...")
            start_time = time.time()
            
            self.components['websocket'].start()
            
            initialization_time = time.time() - start_time
            logger.info(f"✅ Market data initialization completed in {initialization_time:.1f} seconds")
            
            self.initialization_complete = True
            
            time.sleep(5)
            
            self.signal_thread = threading.Thread(target=self._enhanced_signal_processing)
            self.signal_thread.daemon = True
            self.signal_thread.start()
            
            self.telegram_thread = threading.Thread(target=self._process_telegram_queue)
            self.telegram_thread.daemon = True
            self.telegram_thread.start()
            
            if 'telegram' in self.components:
                try:
                    logger.info("🤖 Starting Telegram bot")
                    self.components['telegram'].start()
                    logger.info("✅ Telegram bot started successfully")
                except Exception as e:
                    logger.error(f"❌ Error starting Telegram bot: {e}")
                    del self.components['telegram']
            
            logger.info("🎉 Enhanced trading bot started successfully")
            
        except Exception as e:
            self.running = False
            logger.error(f"❌ Error starting enhanced trading bot: {e}")
            raise
    
    def _enhanced_signal_processing(self):
        """Enhanced signal processing with better performance tracking"""
        timeframes = ["1m", "5m", "15m", "1h", "4h"]
        
        if not self.initialization_complete:
            logger.info("⏳ Waiting for initialization to complete...")
            while not self.initialization_complete and self.running:
                time.sleep(5)
        
        logger.info("🎯 Enhanced signal processing ready - starting market analysis")
        
        total_symbols = len(self.components['websocket'].symbols) if hasattr(self.components['websocket'], 'symbols') else 0
        logger.info(f"📈 Ready to analyze {total_symbols} symbols for trading signals")
        
        check_interval = 60
        symbols_per_batch = 10
        cycle_count = 0
        
        while self.running:
            try:
                cycle_count += 1
                cycle_start_time = time.time()
                processed_symbols = 0
                signals_generated = 0
                
                symbols = self.components['websocket'].get_active_symbols()
                
                if not symbols:
                    symbols = getattr(self.components['websocket'], 'symbols', [])[:50]
                    if not symbols:
                        logger.warning("⚠️ No active symbols available, retrying in 30 seconds...")
                        time.sleep(30)
                        continue
                
                total_batches = (len(symbols) + symbols_per_batch - 1) // symbols_per_batch
                
                logger.info(f"🔍 Starting analysis cycle #{cycle_count} for {len(symbols)} symbols")
                
                for batch_idx in range(0, min(50, len(symbols)), symbols_per_batch):
                    batch_symbols = symbols[batch_idx:batch_idx+symbols_per_batch]
                    current_batch = batch_idx // symbols_per_batch + 1
                    
                    logger.info(f"📊 Analyzing batch {current_batch}/{min(total_batches, 5)}: {batch_symbols}")
                    
                    for symbol in batch_symbols:
                        processed_symbols += 1
                        
                        klines_data = {}
                        for timeframe in timeframes:
                            klines = self.components['websocket'].get_historical_klines(symbol, timeframe)
                            if klines:
                                klines_data[timeframe] = klines
                        
                        if not klines_data:
                            logger.debug(f"⚠️ No klines data available for {symbol}")
                            continue
                        
                        try:
                            indicators_result = self.components['indicators'].calculate_indicators(symbol, "1h")
                            if not indicators_result:
                                logger.debug(f"⚠️ Failed to calculate indicators for {symbol}")
                                continue
                        except Exception as e:
                            logger.debug(f"⚠️ Failed to calculate indicators for {symbol}: {e}")
                            continue
                        
                        try:
                            signals = self.components['signals'].generate_signals(symbol, timeframes, klines_data)
                            
                            if signals:
                                signals_generated += len(signals)
                                logger.info(f"🎯 Generated {len(signals)} signals for {symbol}")
                                
                                for signal in signals:
                                    self.signal_queue.put(signal)
                            
                        except Exception as e:
                            logger.debug(f"❌ Error generating signals for {symbol}: {e}")
                    
                    time.sleep(2)
                
                cycle_time = time.time() - cycle_start_time
                symbols_per_minute = (processed_symbols / cycle_time) * 60 if cycle_time > 0 else 0
                
                logger.info(f"✅ Cycle #{cycle_count} completed:")
                logger.info(f"   📊 Processed: {processed_symbols} symbols in {cycle_time:.1f}s")
                logger.info(f"   🎯 Generated: {signals_generated} signals")
                logger.info(f"   ⚡ Rate: {symbols_per_minute:.1f} symbols/minute")
                logger.info(f"   📈 Queue: {self.signal_queue.qsize()} signals pending")
                logger.info(f"   ⏰ Next cycle in {check_interval} seconds")
                
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"❌ Error in enhanced signal processing: {e}")
                time.sleep(30)
    
    def _process_telegram_queue(self):
        """Process Telegram queue with enhanced logging"""
        while self.running:
            try:
                try:
                    signal = self.signal_queue.get(timeout=5)
                    if 'telegram' in self.components:
                        try:
                            logger.info(f"📤 Sending signal to Telegram: {signal}")
                            self.components['telegram'].send_signal_notification(signal)
                        except Exception as e:
                            logger.error(f"❌ Error sending signal to Telegram: {e}")
                    self.signal_queue.task_done()
                except queue.Empty:
                    pass
            except Exception as e:
                logger.error(f"❌ Error in Telegram queue processing: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop enhanced trading bot"""
        if not self.running:
            return
        
        self.running = False
        logger.info("🛑 Stopping enhanced trading bot")
        
        try:
            if 'telegram' in self.components:
                self.components['telegram'].stop()
            
            if 'websocket' in self.components:
                self.components['websocket'].stop()
            
            if 'database' in self.components:
                self.components['database'].close_all()
            
            logger.info("✅ Enhanced trading bot stopped successfully")
            
        except Exception as e:
            logger.error(f"❌ Error stopping enhanced trading bot: {e}")
    
    def run_forever(self):
        """Run enhanced bot with comprehensive status monitoring"""
        self.start()
        
        def signal_handler(sig, frame):
            logger.info("🛑 Shutdown signal received")
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("🚀 Enhanced trading bot running. Press Ctrl+C to stop.")
        logger.info(f"🔧 Active components: {list(self.components.keys())}")
        
        status_interval = 180
        last_status_time = time.time()
        
        while self.running:
            current_time = time.time()
            if current_time - last_status_time > status_interval:
                
                if 'telegram' in self.components:
                    telegram_status = "✅ Running" if hasattr(self.components['telegram'], 'running') and self.components['telegram'].running else "❌ Not running"
                    logger.info(f"🤖 Telegram bot status: {telegram_status}")
                else:
                    logger.info(f"🤖 Telegram bot status: ❌ Not configured")
                
                websocket_status = "✅ Running" if self.components['websocket'].is_running() else "❌ Not running"
                logger.info(f"🌐 WebSocket client status: {websocket_status}")
                
                active_connections = len(self.components['websocket'].active_websockets) if hasattr(self.components['websocket'], 'active_websockets') else 0
                logger.info(f"🔗 Active WebSocket connections: {active_connections}")
                
                queue_size = self.signal_queue.qsize()
                logger.info(f"📊 Signals in queue: {queue_size}")
                
                if hasattr(self.components['websocket'], 'symbols'):
                    cached_symbols = len(self.components['websocket'].symbols)
                    logger.info(f"💾 Cached market data for {cached_symbols} symbols")
                
                logger.info(f"🚀 Enhanced bot operational - ready to generate trading signals!")
                last_status_time = current_time
            
            time.sleep(1)


if __name__ == "__main__":
    TELEGRAM_TOKEN = "7633147170:AAGTGmkeVSnCdBnO8d5Pzxx7v7WzNBfhSNI"
    ALLOWED_USERS = [5829074137]
    
    logger.info(f"🎯 Allowed Telegram users: {ALLOWED_USERS}")
    
    bot = EnhancedTradingBot(telegram_token=TELEGRAM_TOKEN, allowed_users=ALLOWED_USERS)
    bot.run_forever()
