#!/usr/bin/env python3
"""Test enhanced components functionality"""

import sys
import logging
from enhanced_database_manager import EnhancedDatabaseManager
from performance_enhanced_websocket import PerformanceEnhancedWebSocketClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_enhanced_components():
    """Test enhanced components"""
    try:
        logger.info("✅ Enhanced imports work")
        
        db_manager = EnhancedDatabaseManager('test_data')
        db_manager.initialize_schema()
        logger.info("✅ Enhanced database schema initialized")
        
        if db_manager.table_exists('market_data', 'kline_data'):
            logger.info("✅ kline_data table created successfully")
        else:
            logger.error("❌ kline_data table not found")
            return False
        
        websocket_client = PerformanceEnhancedWebSocketClient(db_manager)
        logger.info("✅ Enhanced WebSocket client initialized")
        
        logger.info("🎉 All enhanced components working correctly!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error testing enhanced components: {e}")
        return False

if __name__ == "__main__":
    success = test_enhanced_components()
    sys.exit(0 if success else 1)
