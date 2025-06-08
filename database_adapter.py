import logging
import sqlite3
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseAdapter:
    def __init__(self, db_path='data'):
        self.db_path = db_path
        os.makedirs(db_path, exist_ok=True)
        logger.info("Database adapter initialized")

        self._create_tables()
        self._add_confirmed_column_if_not_exists()

    def _add_confirmed_column_if_not_exists(self):
        """Add the confirmed column to positions table if it doesn't exist"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(positions)")
            columns = [column[1] for column in cursor.fetchall()]

            if 'confirmed' not in columns:
                logger.info("Adding 'confirmed' column to positions table")
                cursor.execute("ALTER TABLE positions ADD COLUMN confirmed BOOLEAN DEFAULT 0")
                conn.commit()

            conn.close()
        except Exception as e:
            logger.error(f"Error adding 'confirmed' column: {e}")

    def _create_tables(self):
        """Create database tables if they don't exist"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                position_type TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_time TEXT NOT NULL,
                exit_price REAL,
                exit_time TEXT,
                status TEXT DEFAULT 'OPEN',
                profit_loss_percent REAL,
                exit_reason TEXT,
                confirmed BOOLEAN DEFAULT 0,
                signal_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL,
                entry_price REAL,
                timestamp INTEGER,
                status TEXT DEFAULT 'PENDING'
            )
            ''')

            conn.commit()
            conn.close()
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")

    def increment_signal_count(self, symbol):
        """Increment the signal count for a symbol and check if it's below the limit"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'config.db'))
            cursor = conn.cursor()
            cursor.execute('SELECT max_signals_per_day FROM configuration WHERE id = 1')
            max_signals = cursor.fetchone()[0]
            conn.close()

            today = datetime.now().strftime('%Y-%m-%d')
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()
            cursor.execute('''
            SELECT COUNT(*) FROM signals 
            WHERE symbol = ? AND date(created_at) = ?
            ''', (symbol, today))

            count = cursor.fetchone()[0]
            conn.close()

            return count < max_signals
        except Exception as e:
            logger.error(f"Error incrementing signal count: {e}")
            return False

    def add_position(self, symbol, price, trend, signal_id):
        """Add a new position to the database"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()

            cursor.execute('''
            INSERT INTO signals (
                symbol, direction, confidence, entry_price, timestamp, status
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                trend,
                0.8,  # Default confidence score
                price,
                int(datetime.now().timestamp() * 1000),
                'SENT'
            ))

            cursor.execute('SELECT last_insert_rowid()')
            db_signal_id = cursor.fetchone()[0]

            cursor.execute('''
            INSERT INTO positions (
                symbol, position_type, entry_price, entry_time, status, confirmed, signal_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                trend,
                price,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'OPEN',
                0,  # Not confirmed by default
                db_signal_id
            ))

            conn.commit()
            cursor.execute('SELECT last_insert_rowid()')
            position_id = cursor.fetchone()[0]
            conn.close()

            return position_id
        except Exception as e:
            logger.error(f"Error adding position: {e}")
            return None

    def confirm_position(self, signal_id):
        """Mark a position as confirmed by the user"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()

            cursor.execute('''
            SELECT id FROM positions
            WHERE signal_id = ?
            ''', (signal_id,))

            position = cursor.fetchone()

            if not position:
                cursor.execute('''
                SELECT id FROM positions
                WHERE id = ?
                ''', (signal_id,))

                position = cursor.fetchone()

                if not position:
                    logger.error(f"No position found with signal_id or id {signal_id}")
                    conn.close()
                    return False

            cursor.execute('''
            UPDATE positions
            SET confirmed = 1
            WHERE id = ?
            ''', (position[0],))

            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()

            return rows_affected > 0
        except Exception as e:
            logger.error(f"Error confirming position: {e}")
            return False

    def close_position(self, position_id, exit_price):
        """Close a position in the database"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()

            cursor.execute('''
            SELECT symbol, position_type, entry_price, signal_id
            FROM positions WHERE id = ? AND status = 'OPEN'
            ''', (position_id,))

            position = cursor.fetchone()
            if not position:
                conn.close()
                return False

            symbol, position_type, entry_price, signal_id = position

            if position_type == 'LONG':
                profit_pct = ((exit_price - entry_price) / entry_price) * 100
            else:  # SHORT
                profit_pct = ((entry_price - exit_price) / entry_price) * 100

            cursor.execute('''
            UPDATE positions SET 
                exit_price = ?, 
                exit_time = ?,
                status = 'CLOSED',
                profit_loss_percent = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''', (
                exit_price,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                profit_pct,
                position_id
            ))

            cursor.execute('''
            UPDATE signals SET status = 'COMPLETED' WHERE id = ?
            ''', (signal_id,))

            conn.commit()
            conn.close()

            return True
        except Exception as e:
            logger.error(f"Error closing position: {e}")
            return False

    def get_open_positions(self):
        """Get all confirmed open positions from the database"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()

            cursor.execute('''
            SELECT id, symbol, entry_price, position_type, entry_time, signal_id
            FROM positions
            WHERE status = 'OPEN' AND confirmed = 1
            ''')

            positions = cursor.fetchall()
            conn.close()

            return positions
        except Exception as e:
            logger.error(f"Error getting open positions: {e}")
            return []

    def get_position_by_signal_id(self, signal_id):
        """Get a position by its signal ID"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'positions.db'))
            cursor = conn.cursor()

            cursor.execute('''
            SELECT id, symbol, entry_price, position_type, entry_time, signal_id
            FROM positions
            WHERE signal_id = ?
            ''', (signal_id,))

            position = cursor.fetchone()
            conn.close()

            return position
        except Exception as e:
            logger.error(f"Error getting position by signal ID: {e}")
            return None

    def set_max_signals(self, symbol, max_count):
        """Set the maximum number of signals per day for a symbol"""
        try:
            conn = sqlite3.connect(os.path.join(self.db_path, 'config.db'))
            cursor = conn.cursor()

            cursor.execute('''
            SELECT COUNT(*) FROM symbol_config WHERE symbol = ?
            ''', (symbol,))

            if cursor.fetchone()[0] > 0:
                cursor.execute('''
                UPDATE symbol_config 
                SET max_signals = ?, updated_at = CURRENT_TIMESTAMP
                WHERE symbol = ?
                ''', (max_count, symbol))
            else:
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS symbol_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT UNIQUE NOT NULL,
                    max_signals INTEGER NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                ''')

                cursor.execute('''
                INSERT INTO symbol_config (symbol, max_signals)
                VALUES (?, ?)
                ''', (symbol, max_count))

            conn.commit()
            conn.close()

            return True
        except Exception as e:
            logger.error(f"Error setting max signals for {symbol}: {e}")
            return False
