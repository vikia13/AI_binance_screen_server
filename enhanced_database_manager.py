import os
import sqlite3
import threading
import logging
import time

logger = logging.getLogger(__name__)

class EnhancedDatabaseManager:
    """Enhanced database manager with improved schema and performance"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection_pool = {}
        self.lock = threading.Lock()
        
        os.makedirs(db_path, exist_ok=True)
        logger.info("Enhanced database manager initialized")
    
    def get_connection(self, db_name):
        """Get thread-specific database connection"""
        thread_id = threading.get_ident()
        
        with self.lock:
            if thread_id not in self.connection_pool:
                self.connection_pool[thread_id] = {}
            
            if db_name not in self.connection_pool[thread_id]:
                db_file = os.path.join(self.db_path, f"{db_name}.db")
                conn = sqlite3.connect(db_file, timeout=30)
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = NORMAL")
                conn.execute("PRAGMA cache_size = 10000")
                conn.row_factory = sqlite3.Row
                self.connection_pool[thread_id][db_name] = conn
                logger.debug(f"Created enhanced connection for thread {thread_id}, database {db_name}")
            
            return self.connection_pool[thread_id][db_name]
    
    def execute_query(self, db_name, query, params=None, fetch=None, commit=True, max_retries=5):
        """Execute SQL query with enhanced retry logic"""
        conn = self.get_connection(db_name)
        cursor = conn.cursor()
        
        for attempt in range(max_retries):
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if commit:
                    conn.commit()
                
                if fetch == 'one':
                    return cursor.fetchone()
                elif fetch == 'all':
                    return cursor.fetchall()
                elif fetch == 'lastrowid':
                    return cursor.lastrowid
                else:
                    return True
                    
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    sleep_time = 0.1 * (2 ** attempt)
                    logger.warning(f"Database locked, retrying in {sleep_time:.2f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Database error after {attempt+1} attempts: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                raise
    
    def initialize_schema(self):
        """Initialize enhanced database schema with all required tables"""
        logger.info("🔧 Initializing enhanced database schema...")
        
        self.execute_query('market_data', 'DROP TABLE IF EXISTS market_data')
        
        self.execute_query('market_data', '''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            timeframe TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            UNIQUE(symbol, timestamp, timeframe)
        )
        ''')
        
        self.execute_query('market_data', '''
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
        
        self.execute_query('market_data', '''
        CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time 
        ON market_data(symbol, timestamp)
        ''')
        
        self.execute_query('market_data', '''
        CREATE INDEX IF NOT EXISTS idx_kline_data_symbol_time 
        ON kline_data(symbol, timeframe, open_time)
        ''')
        
        logger.info("✅ Enhanced database schema initialized with market_data and kline_data tables")
    
    def table_exists(self, db_name, table_name):
        """Check if table exists"""
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = self.execute_query(db_name, query, (table_name,), fetch='one')
        return result is not None
    
    def close_all(self):
        """Close all database connections"""
        with self.lock:
            for thread_id, connections in list(self.connection_pool.items()):
                for db_name, conn in list(connections.items()):
                    try:
                        conn.close()
                        logger.debug(f"Closed connection for thread {thread_id}, database {db_name}")
                    except Exception as e:
                        logger.error(f"Error closing connection to {db_name}: {e}")
            
            self.connection_pool = {}
            logger.info("All enhanced database connections closed")
