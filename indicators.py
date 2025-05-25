import pandas as pd
import numpy as np
import os
import logging
import time
import threading
from typing import Optional, Dict, Any, Union, List

logger = logging.getLogger(__name__)

class TechnicalIndicators:
    """Technical analysis indicators module with improved database access"""

    def __init__(self, db_manager):
        try:
            # Store both the database manager and path
            self.db_manager = db_manager  # Store the database manager object

            if isinstance(db_manager, str):
                self.db_path = db_manager
            else:
                # Assuming DatabaseManager has a db_path attribute
                self.db_path = db_manager.db_path

            # Create technical analysis directory
            os.makedirs(os.path.join(self.db_path, 'technical_analysis'), exist_ok=True)
            logger.info("Technical indicators module initialized")
        except Exception as e:
            logger.error(f"Database manager not provided to TechnicalIndicators: {e}")

    def calculate_indicators(self, symbol: str, timeframe: str = '1h', limit: int = 100) -> Optional[Dict[str, Any]]:
        """Calculate all technical indicators for a symbol and timeframe"""
        if not self.db_manager:
            logger.error("Database manager not available")
            return None

        try:
            # Get kline data from market_data database
            params = {'symbol': symbol, 'timeframe': timeframe, 'limit': limit + 100}
            result = self.db_manager.execute_query(
                'market_data',
                '''
                SELECT open_time, open_price, high_price, low_price, close_price, volume, timestamp
                FROM kline_data
                WHERE symbol = :symbol AND timeframe = :timeframe
                ORDER BY open_time ASC
                LIMIT :limit
                ''',
                params=params,
                fetch='all'
            )

            if not result or len(result) < 50:  # Need minimum data for accurate indicators
                logger.warning(f"Not enough data for {symbol} {timeframe} indicators")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(result, columns=['open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'timestamp'])

            # Calculate indicators
            df = self._calculate_rsi(df)
            df = self._calculate_macd(df)
            df = self._calculate_ema(df)
            df = self._calculate_adx(df)
            df = self._calculate_volume_change(df)

            # Only store the most recent data points (limit)
            df = df.tail(limit)

            # Store in database
            for _, row in df.iterrows():
                if pd.isna(row['rsi']) or pd.isna(row['macd']):
                    continue

                self.db_manager.execute_query(
                    'technical_analysis',
                    '''
                    INSERT OR REPLACE INTO indicators
                    (symbol, timeframe, timestamp, rsi, macd, macd_signal, macd_histogram,
                    ema_short, ema_medium, ema_long, ema_crossover_short_medium,
                    ema_crossover_medium_long, adx, volume_change_24h, price_change_24h)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    params=(
                        symbol,
                        timeframe,
                        int(row['timestamp']),
                        float(row['rsi']),
                        float(row['macd']),
                        float(row['macd_signal']),
                        float(row['macd_histogram']),
                        float(row['ema_short']),
                        float(row['ema_medium']),
                        float(row['ema_long']),
                        int(row['ema_crossover_short_medium']),
                        int(row['ema_crossover_medium_long']),
                        float(row['adx']),
                        float(row['volume_change_24h']),
                        float(row['price_change_24h'])
                    )
                )

            return df.tail(1).to_dict('records')[0]

        except Exception as e:
            logger.error(f"Error calculating indicators for {symbol} {timeframe}: {e}")
            return None

    @staticmethod
    def _calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Calculate Relative Strength Index"""
        delta = df['close_price'].diff()
        gain = delta.mask(delta < 0, 0)
        loss = -delta.mask(delta > 0, 0)

        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()

        # Calculate RS (Relative Strength)
        rs = avg_gain / avg_loss

        # Calculate RSI
        df['rsi'] = 100 - (100 / (1 + rs))

        return df

    @staticmethod
    def _calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """Calculate MACD (Moving Average Convergence Divergence)"""
        # Calculate EMAs
        ema_fast = df['close_price'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close_price'].ewm(span=slow, adjust=False).mean()

        # Calculate MACD line
        df['macd'] = ema_fast - ema_slow

        # Calculate signal line
        df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()

        # Calculate MACD histogram
        df['macd_histogram'] = df['macd'] - df['macd_signal']

        return df

    @staticmethod
    def _calculate_ema(df: pd.DataFrame, short: int = 9, medium: int = 21, long: int = 50) -> pd.DataFrame:
        """Calculate EMA crossovers"""
        # Calculate EMAs
        df['ema_short'] = df['close_price'].ewm(span=short, adjust=False).mean()
        df['ema_medium'] = df['close_price'].ewm(span=medium, adjust=False).mean()
        df['ema_long'] = df['close_price'].ewm(span=long, adjust=False).mean()

        # Calculate crossovers
        df['ema_crossover_short_medium'] = 0  # 0 = no crossover
        df['ema_crossover_medium_long'] = 0

        # Short-Medium crossover (1 = bullish, -1 = bearish)
        for i in range(1, len(df)):
            if (df['ema_short'].iloc[i - 1] < df['ema_medium'].iloc[i - 1] and
                    df['ema_short'].iloc[i] >= df['ema_medium'].iloc[i]):
                df.at[df.index[i], 'ema_crossover_short_medium'] = 1  # Bullish crossover
            elif (df['ema_short'].iloc[i - 1] > df['ema_medium'].iloc[i - 1] and
                  df['ema_short'].iloc[i] <= df['ema_medium'].iloc[i]):
                df.at[df.index[i], 'ema_crossover_short_medium'] = -1  # Bearish crossover

        # Medium-Long crossover
        for i in range(1, len(df)):
            if (df['ema_medium'].iloc[i - 1] < df['ema_long'].iloc[i - 1] and
                    df['ema_medium'].iloc[i] >= df['ema_long'].iloc[i]):
                df.at[df.index[i], 'ema_crossover_medium_long'] = 1  # Bullish crossover
            elif (df['ema_medium'].iloc[i - 1] > df['ema_long'].iloc[i - 1] and
                  df['ema_medium'].iloc[i] <= df['ema_long'].iloc[i]):
                df.at[df.index[i], 'ema_crossover_medium_long'] = -1  # Bearish crossover

        return df

    @staticmethod
    def _calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Calculate Average Directional Index"""
        # Calculate True Range
        df['tr1'] = abs(df['high_price'] - df['low_price'])
        df['tr2'] = abs(df['high_price'] - df['close_price'].shift())
        df['tr3'] = abs(df['low_price'] - df['close_price'].shift())
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)

        # Calculate directional movement
        df['dm_plus'] = 0.0
        df['dm_minus'] = 0.0

        for i in range(1, len(df)):
            high_diff = df['high_price'].iloc[i] - df['high_price'].iloc[i - 1]
            low_diff = df['low_price'].iloc[i - 1] - df['low_price'].iloc[i]

            if high_diff > low_diff and high_diff > 0:
                df.at[df.index[i], 'dm_plus'] = high_diff
            else:
                df.at[df.index[i], 'dm_plus'] = 0

            if low_diff > high_diff and low_diff > 0:
                df.at[df.index[i], 'dm_minus'] = low_diff
            else:
                df.at[df.index[i], 'dm_minus'] = 0

        # Calculate smoothed values
        df['smoothed_tr'] = df['tr'].rolling(window=period).sum()
        df['smoothed_dm_plus'] = df['dm_plus'].rolling(window=period).sum()
        df['smoothed_dm_minus'] = df['dm_minus'].rolling(window=period).sum()

        # Calculate directional indicators
        df['di_plus'] = 100 * df['smoothed_dm_plus'] / df['smoothed_tr']
        df['di_minus'] = 100 * df['smoothed_dm_minus'] / df['smoothed_tr']

        # Calculate directional index
        df['dx'] = 100 * abs(df['di_plus'] - df['di_minus']) / (df['di_plus'] + df['di_minus'])

        # Calculate ADX
        df['adx'] = df['dx'].rolling(window=period).mean()

        return df

    @staticmethod
    def _calculate_volume_change(df: pd.DataFrame, period: int = 24) -> pd.DataFrame:
        """Calculate volume and price change over period"""
        # Calculate 24h volume change
        df['volume_change_24h'] = df['volume'].pct_change(periods=period) * 100

        # Calculate 24h price change
        df['price_change_24h'] = df['close_price'].pct_change(periods=period) * 100

        return df

    def get_latest_indicators(self, symbol: str, timeframe: str = '1h') -> Optional[Dict[str, Any]]:
        """Get latest technical indicators for a symbol and timeframe"""
        if not self.db_manager:
            logger.error("Database manager not available")
            return None

        try:
            result = self.db_manager.execute_query(
                'technical_analysis',
                '''
                SELECT rsi, macd, macd_signal, macd_histogram, ema_short, ema_medium, ema_long,
                       ema_crossover_short_medium, ema_crossover_medium_long, adx,
                       volume_change_24h, price_change_24h, timestamp
                FROM indicators
                WHERE symbol = ? AND timeframe = ?
                ORDER BY timestamp DESC
                LIMIT 1
                ''',
                params=(symbol, timeframe),
                fetch='one'
            )

            if not result:
                return None

            return {
                'symbol': symbol,
                'timeframe': timeframe,
                'rsi': result[0],
                'macd': result[1],
                'macd_signal': result[2],
                'macd_histogram': result[3],
                'ema_short': result[4],
                'ema_medium': result[5],
                'ema_long': result[6],
                'ema_crossover_short_medium': result[7],
                'ema_crossover_medium_long': result[8],
                'adx': result[9],
                'volume_change_24h': result[10],
                'price_change_24h': result[11],
                'timestamp': result[12]
            }
        except Exception as e:
            logger.error(f"Error getting latest indicators for {symbol} {timeframe}: {e}")
            return None

    def generate_features_for_ai(self, symbol: str, timeframe: str = '1h') -> Optional[int]:
        """Generate features for AI model based on technical indicators"""
        if not self.db_manager:
            logger.error("Database manager not available")
            return None

        try:
            # Get latest indicators
            indicators = self.get_latest_indicators(symbol, timeframe)

            if not indicators:
                return None

            # Create table if it doesn't exist
            self.db_manager.execute_query(
                'ai_model',
                '''
                CREATE TABLE IF NOT EXISTS model_features (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    price_change_1m REAL,
                    price_change_5m REAL,
                    price_change_15m REAL,
                    price_change_1h REAL,
                    volume_change_1m REAL,
                    volume_change_5m REAL,
                    rsi_value REAL,
                    macd_histogram REAL,
                    ema_crossover INTEGER,
                    timestamp INTEGER NOT NULL
                )
                '''
            )

            # Get price changes for multiple timeframes
            price_changes = self._get_price_changes(symbol)

            # Insert features
            result = self.db_manager.execute_query(
                'ai_model',
                '''
                INSERT INTO model_features
                (symbol, price_change_1m, price_change_5m, price_change_15m, price_change_1h,
                 volume_change_1m, volume_change_5m, rsi_value, macd_histogram, ema_crossover, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                params=(
                    symbol,
                    price_changes.get('1m', 0),
                    price_changes.get('5m', 0),
                    price_changes.get('15m', 0),
                    price_changes.get('1h', 0),
                    price_changes.get('vol_1m', 0),
                    price_changes.get('vol_5m', 0),
                    indicators['rsi'],
                    indicators['macd_histogram'],
                    indicators['ema_crossover_medium_long'],  # Use medium-long term crossover
                    indicators['timestamp']
                ),
                fetch='lastrowid'
            )

            return result
        except Exception as e:
            logger.error(f"Error generating features for {symbol}: {e}")
            return None

    def _get_price_changes(self, symbol: str) -> Dict[str, float]:
        """Get price changes for multiple timeframes"""
        if not self.db_manager:
            logger.error("Database manager not available")
            return {}

        try:
            timeframes = {
                '1m': 1 * 60 * 1000,  # 1 minute in milliseconds
                '5m': 5 * 60 * 1000,
                '15m': 15 * 60 * 1000,
                '1h': 60 * 60 * 1000
            }

            result = {}
            timestamp_now = int(time.time() * 1000)

            # Get latest price
            latest = self.db_manager.execute_query(
                'market_data',
                '''
                SELECT price, volume, timestamp
                FROM market_data
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                ''',
                params=(symbol,),
                fetch='one'
            )

            if not latest:
                return result

            latest_price, latest_volume, latest_timestamp = latest

            # Calculate price changes for each timeframe
            for tf_name, tf_ms in timeframes.items():
                # Get historical price
                historical = self.db_manager.execute_query(
                    'market_data',
                    '''
                    SELECT price, volume
                    FROM market_data
                    WHERE symbol = ? AND timestamp < ?
                    ORDER BY ABS(timestamp - ?) ASC
                    LIMIT 1
                    ''',
                    params=(symbol, timestamp_now - tf_ms, timestamp_now - tf_ms),
                    fetch='one'
                )

                if historical:
                    hist_price, hist_volume = historical

                    # Calculate price change
                    if hist_price > 0:
                        price_change = ((latest_price - hist_price) / hist_price) * 100
                        result[tf_name] = price_change

                    # Calculate volume change
                    if hist_volume > 0:
                        volume_change = ((latest_volume - hist_volume) / hist_volume) * 100
                        result[f'vol_{tf_name}'] = volume_change

            return result
        except Exception as e:
            logger.error(f"Error getting price changes for {symbol}: {e}")
            return {}