import os
import sqlite3
import threading
import logging
import time

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database connection manager to prevent SQLite locking issues.
    Manages thread-specific connections and implements retry logic.
    """

    def __init__(self, db_path):
        """
        Initialize the database manager.

        Args:
            db_path: Path to the directory where database files are stored
        """
        self.db_path = db_path
        self.connection_pool = {}
        self.lock = threading.Lock()

        # Ensure the database directory exists
        os.makedirs(db_path, exist_ok=True)

    def get_connection(self, db_name):
        """
        Get a thread-specific database connection.

        Args:
            db_name: Name of the database (without .db extension)

        Returns:
            SQLite connection object
        """
        thread_id = threading.get_ident()

        with self.lock:
            if thread_id not in self.connection_pool:
                self.connection_pool[thread_id] = {}

            if db_name not in self.connection_pool[thread_id]:
                db_file = os.path.join(self.db_path, f"{db_name}.db")
                conn = sqlite3.connect(db_file, timeout=30)
                # Enable foreign keys
                conn.execute("PRAGMA foreign_keys = ON")
                # Set journal mode to WAL for better concurrency
                conn.execute("PRAGMA journal_mode = WAL")
                conn.row_factory = sqlite3.Row
                self.connection_pool[thread_id][db_name] = conn
                logger.debug(f"Created new connection for thread {thread_id}, database {db_name}")

            return self.connection_pool[thread_id][db_name]

    def execute_query(self, db_name, query, params=None, fetch=None, commit=True, max_retries=5):
        """
        Execute SQL query with retry logic for database locks.

        Args:
            db_name: Name of the database (without .db extension)
            query: SQL query string
            params: Parameters for the query (optional)
            fetch: Fetch mode - 'one', 'all', 'lastrowid', or None
            commit: Whether to commit the transaction
            max_retries: Maximum number of retries for locked database

        Returns:
            Query results based on fetch parameter
        """
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
                    # Use exponential backoff for retries
                    sleep_time = 0.1 * (2 ** attempt)
                    logger.warning(f"Database locked, retrying in {sleep_time:.2f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Database error after {attempt+1} attempts: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                raise

    def execute_script(self, db_name, script, max_retries=5):
        """
        Execute SQL script with retry logic for database locks.

        Args:
            db_name: Name of the database (without .db extension)
            script: SQL script string
            max_retries: Maximum number of retries for locked database

        Returns:
            True if successful
        """
        conn = self.get_connection(db_name)

        for attempt in range(max_retries):
            try:
                conn.executescript(script)
                conn.commit()
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
                logger.error(f"Error executing script: {e}")
                raise

    def create_table(self, db_name, table_name, schema):
        """
        Create a table if it does not exist.

        Args:
            db_name: Name of the database (without .db extension)
            table_name: Name of the table to create
            schema: Schema definition (column specifications)

        Returns:
            True if successful
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        return self.execute_query(db_name, query)

    def initialize_schema(self):
        """Create or update the database tables with proper schema"""
        # Drop existing table to ensure correct schema
        self.execute_query('market_data', 'DROP TABLE IF EXISTS market_data')

        # Initialize market_data table with correct columns
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
    def table_exists(self, db_name, table_name):
        """
        Check if a table exists in the database.

        Args:
            db_name: Name of the database (without .db extension)
            table_name: Name of the table to check

        Returns:
            True if the table exists
        """
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = self.execute_query(db_name, query, (table_name,), fetch='one')
        return result is not None

    def vacuum_database(self, db_name):
        """
        Run VACUUM on database to optimize storage.

        Args:
            db_name: Name of the database (without .db extension)

        Returns:
            True if successful
        """
        return self.execute_query(db_name, "VACUUM")

    def close_connection(self, db_name, thread_id=None):
        """
        Close a specific database connection.

        Args:
            db_name: Name of the database (without .db extension)
            thread_id: Thread ID (optional, current thread if None)
        """
        if thread_id is None:
            thread_id = threading.get_ident()

        with self.lock:
            if thread_id in self.connection_pool and db_name in self.connection_pool[thread_id]:
                try:
                    self.connection_pool[thread_id][db_name].close()
                    del self.connection_pool[thread_id][db_name]
                    logger.debug(f"Closed connection for thread {thread_id}, database {db_name}")
                except Exception as e:
                    logger.error(f"Error closing database connection: {e}")

    def close_all(self):
        """Close all database connections in the pool"""
        with self.lock:
            for thread_id, connections in list(self.connection_pool.items()):
                for db_name, conn in list(connections.items()):
                    try:
                        conn.close()
                        logger.debug(f"Closed connection for thread {thread_id}, database {db_name}")
                    except Exception as e:
                        logger.error(f"Error closing connection to {db_name}: {e}")

            self.connection_pool = {}
            logger.info("All database connections closed")