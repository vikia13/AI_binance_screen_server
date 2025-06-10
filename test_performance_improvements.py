#!/usr/bin/env python3
"""Test script for performance improvements"""

import os
import sys
import time
import logging
import threading
from enhanced_database_manager import EnhancedDatabaseManager
from performance_enhanced_websocket import PerformanceEnhancedWebSocketClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_performance():
    """Test enhanced database performance"""
    logger.info("🧪 Testing enhanced database performance...")
    
    db_manager = EnhancedDatabaseManager('test_data')
    
    start_time = time.time()
    db_manager.initialize_schema()
    schema_time = time.time() - start_time
    
    logger.info(f"✅ Database schema initialization: {schema_time:.2f}s")
    
    if db_manager.table_exists('market_data', 'kline_data'):
        logger.info("✅ kline_data table created successfully")
    else:
        logger.error("❌ kline_data table not found")
        return False
    
    db_manager.close_all()
    return True

def test_websocket_performance():
    """Test enhanced WebSocket performance"""
    logger.info("🧪 Testing enhanced WebSocket performance...")
    
    db_manager = EnhancedDatabaseManager('test_data')
    websocket_client = PerformanceEnhancedWebSocketClient(db_manager)
    
    start_time = time.time()
    
    def run_test():
        websocket_client.start()
    
    test_thread = threading.Thread(target=run_test)
    test_thread.daemon = True
    test_thread.start()
    
    test_thread.join(timeout=120)
    
    total_time = time.time() - start_time
    
    if websocket_client.is_running():
        logger.info(f"✅ WebSocket initialization completed in {total_time:.1f}s")
        symbols_count = len(websocket_client.symbols)
        logger.info(f"✅ Loaded data for {symbols_count} symbols")
        
        if symbols_count > 0:
            avg_time = total_time / symbols_count
            logger.info(f"✅ Average time per symbol: {avg_time:.2f}s")
        
        websocket_client.stop()
        return True
    else:
        logger.error("❌ WebSocket initialization failed")
        return False

def test_progress_tracking():
    """Test progress tracking functionality"""
    logger.info("🧪 Testing progress tracking functionality...")
    
    db_manager = EnhancedDatabaseManager('test_data')
    websocket_client = PerformanceEnhancedWebSocketClient(db_manager)
    
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
    websocket_client.symbols = symbols
    
    start_time = time.time()
    success = websocket_client.initialize_market_data_enhanced()
    total_time = time.time() - start_time
    
    if success:
        logger.info(f"✅ Progress tracking test completed in {total_time:.1f}s")
        return True
    else:
        logger.error("❌ Progress tracking test failed")
        return False

def main():
    """Run all performance tests"""
    logger.info("🚀 Starting performance improvement tests...")
    
    tests = [
        ("Database Performance", test_database_performance),
        ("WebSocket Performance", test_websocket_performance),
        ("Progress Tracking", test_progress_tracking)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running: {test_name}")
        logger.info(f"{'='*50}")
        
        try:
            if test_func():
                logger.info(f"✅ {test_name}: PASSED")
                passed += 1
            else:
                logger.error(f"❌ {test_name}: FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name}: ERROR - {e}")
    
    logger.info(f"\n{'='*50}")
    logger.info(f"Test Results: {passed}/{total} tests passed")
    logger.info(f"{'='*50}")
    
    if passed == total:
        logger.info("🎉 All performance tests passed!")
        return 0
    else:
        logger.error(f"❌ {total - passed} tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
