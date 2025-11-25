"""
Stock Market Time-Series Pipeline
Intermediate complexity - Demonstrates temporal data processing and technical indicators
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/config.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockMarketPipeline:
    """Time-series pipeline for stock market data with technical indicators"""
    
    def __init__(self):
        logger.info("🔄 Initializing Stock Market Time-Series Pipeline")
        self.df = None
        self.stocks = ['AAPL', 'GOOGL', 'MSFT']  # Top tech stocks
        logger.info("✅ Pipeline initialized")
    
    def run(self):
        """Execute the complete pipeline"""
        try:
            logger.info("\n" + "="*60)
            logger.info("Starting Stock Market Time-Series Pipeline")
            logger.info("="*60)
            
            # Stage 1: Extract stock price data
            self._stage_extract()
            
            # Stage 2: Calculate technical indicators
            self._stage_technical_indicators()
            
            # Stage 3: Enrich with market context
            self._stage_market_context()
            
            # Stage 4: Load to PostgreSQL
            self._stage_load()
            
            logger.info("\n" + "="*60)
            logger.info("✅ Pipeline completed successfully")
            logger.info("="*60)
            logger.info("\n🎉 Pipeline execution completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            raise
    
    def _stage_extract(self):
        """Stage 1: Extract stock price data from Yahoo Finance API"""
        logger.info("\n📥 Stage 1: Extracting stock price data")
        
        try:
            all_stocks = []
            
            # Calculate date range (90 days of historical data)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)
            
            for symbol in self.stocks:
                logger.info(f"  Fetching data for {symbol}...")
                
                # Yahoo Finance API endpoint
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                params = {
                    'period1': int(start_date.timestamp()),
                    'period2': int(end_date.timestamp()),
                    'interval': '1d',
                    'includePrePost': 'false'
                }
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                response = requests.get(url, params=params, headers=headers)
                
                if response.status_code != 200:
                    logger.warning(f"    ⚠ Failed to fetch {symbol}, status code: {response.status_code}")
                    continue
                    
                data = response.json()
                
                if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
                    result = data['chart']['result'][0]
                    timestamps = result['timestamp']
                    quotes = result['indicators']['quote'][0]
                    
                    # Create DataFrame for this stock
                    stock_df = pd.DataFrame({
                        'timestamp': [datetime.fromtimestamp(ts) for ts in timestamps],
                        'symbol': symbol,
                        'open': quotes['open'],
                        'high': quotes['high'],
                        'low': quotes['low'],
                        'close': quotes['close'],
                        'volume': quotes['volume']
                    })
                    
                    all_stocks.append(stock_df)
                    logger.info(f"    ✓ {symbol}: {len(stock_df)} daily records")
            
            # Combine all stocks
            self.df = pd.concat(all_stocks, ignore_index=True)
            
            # Clean data
            self.df = self.df.dropna()
            self.df['date'] = self.df['timestamp'].dt.date
            
            # Limit to 200 total records (spread across stocks)
            records_per_stock = 200 // len(self.stocks)
            limited_stocks = []
            for symbol in self.stocks:
                stock_data = self.df[self.df['symbol'] == symbol].tail(records_per_stock)
                limited_stocks.append(stock_data)
            self.df = pd.concat(limited_stocks, ignore_index=True)
            
            logger.info(f"✅ Extracted {len(self.df)} total records across {len(self.stocks)} stocks")
            logger.info(f"  Date range: {self.df['date'].min()} to {self.df['date'].max()}")
            
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            raise
    
    def _stage_technical_indicators(self):
        """Stage 2: Calculate technical indicators (MA, RSI, volatility)"""
        logger.info("\n📊 Stage 2: Calculating technical indicators")
        
        try:
            # Sort by symbol and date for proper calculations
            self.df = self.df.sort_values(['symbol', 'timestamp'])
            
            # Calculate for each stock separately
            for symbol in self.stocks:
                mask = self.df['symbol'] == symbol
                stock_data = self.df[mask].copy()
                
                # 1. Daily returns (percentage change)
                stock_data['daily_return'] = stock_data['close'].pct_change() * 100
                
                # 2. Moving averages (7-day and 20-day)
                stock_data['ma_7'] = stock_data['close'].rolling(window=7, min_periods=1).mean()
                stock_data['ma_20'] = stock_data['close'].rolling(window=20, min_periods=1).mean()
                
                # 3. Volatility (7-day rolling standard deviation of returns)
                stock_data['volatility_7d'] = stock_data['daily_return'].rolling(window=7, min_periods=1).std()
                
                # 4. RSI (Relative Strength Index) - simplified 14-day
                delta = stock_data['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
                rs = gain / loss.replace(0, 0.0001)  # Avoid division by zero
                stock_data['rsi'] = 100 - (100 / (1 + rs))
                
                # 5. Price momentum (close vs 7-day MA)
                stock_data['momentum'] = ((stock_data['close'] - stock_data['ma_7']) / stock_data['ma_7'] * 100)
                
                # Update main dataframe
                self.df.loc[mask, 'daily_return'] = stock_data['daily_return']
                self.df.loc[mask, 'ma_7'] = stock_data['ma_7']
                self.df.loc[mask, 'ma_20'] = stock_data['ma_20']
                self.df.loc[mask, 'volatility_7d'] = stock_data['volatility_7d']
                self.df.loc[mask, 'rsi'] = stock_data['rsi']
                self.df.loc[mask, 'momentum'] = stock_data['momentum']
            
            # Fill NaN values with 0 for first few rows
            indicator_cols = ['daily_return', 'ma_7', 'ma_20', 'volatility_7d', 'rsi', 'momentum']
            self.df[indicator_cols] = self.df[indicator_cols].fillna(0)
            
            logger.info("✅ Technical indicators calculated")
            logger.info(f"  Indicators: Daily Return, MA(7), MA(20), Volatility, RSI, Momentum")
            logger.info(f"  Average RSI across all stocks: {self.df['rsi'].mean():.2f}")
            
        except Exception as e:
            logger.error(f"❌ Technical indicator calculation failed: {e}")
            raise
    
    def _stage_market_context(self):
        """Stage 3: Enrich with market context and classifications"""
        logger.info("\n🏷️ Stage 3: Adding market context")
        
        try:
            # 1. Classify trading volume (relative to stock's average)
            for symbol in self.stocks:
                mask = self.df['symbol'] == symbol
                avg_volume = self.df.loc[mask, 'volume'].mean()
                
                self.df.loc[mask, 'volume_category'] = self.df.loc[mask, 'volume'].apply(
                    lambda x: 'high' if x > avg_volume * 1.5
                    else 'low' if x < avg_volume * 0.5
                    else 'normal'
                )
            
            # 2. Trend classification based on MA relationship
            self.df['trend'] = self.df.apply(
                lambda row: 'bullish' if row['close'] > row['ma_20'] and row['ma_7'] > row['ma_20']
                else 'bearish' if row['close'] < row['ma_20'] and row['ma_7'] < row['ma_20']
                else 'neutral',
                axis=1
            )
            
            # 3. RSI signal (overbought/oversold)
            self.df['rsi_signal'] = self.df['rsi'].apply(
                lambda x: 'overbought' if x > 70
                else 'oversold' if x < 30
                else 'neutral'
            )
            
            # 4. Volatility bucket
            volatility_threshold = self.df['volatility_7d'].quantile(0.75)
            self.df['volatility_level'] = self.df['volatility_7d'].apply(
                lambda x: 'high' if x > volatility_threshold
                else 'low' if x < volatility_threshold * 0.5
                else 'medium'
            )
            
            # 5. Price change magnitude
            self.df['price_change'] = self.df['close'] - self.df['open']
            self.df['price_change_pct'] = (self.df['price_change'] / self.df['open'] * 100)
            
            # 6. Day classification
            self.df['day_type'] = self.df['price_change_pct'].apply(
                lambda x: 'strong_gain' if x > 2
                else 'gain' if x > 0
                else 'strong_loss' if x < -2
                else 'loss'
            )
            
            logger.info("✅ Market context added")
            logger.info(f"  Trend distribution: {self.df['trend'].value_counts().to_dict()}")
            logger.info(f"  RSI signals: {self.df['rsi_signal'].value_counts().to_dict()}")
            logger.info(f"  Day types: {self.df['day_type'].value_counts().to_dict()}")
            
        except Exception as e:
            logger.error(f"❌ Market context enrichment failed: {e}")
            raise
    
    def _stage_load(self):
        """Stage 4: Load to PostgreSQL"""
        logger.info("\n💾 Stage 4: Loading to PostgreSQL")
        
        try:
            # Get database URL
            database_url = os.environ.get("AIVEN_PG_URI") or os.environ.get("DATABASE_URL")
            if not database_url:
                raise ValueError("No database URL found in environment variables")
            
            # Fix postgres:// to postgresql:// if needed (SQLAlchemy 1.4+ requires postgresql://)
            if database_url.startswith("postgres://"):
                database_url = database_url.replace("postgres://", "postgresql://", 1)
            
            # Create engine
            engine = create_engine(database_url)
            
            # Prepare final columns
            output_df = self.df[[
                'timestamp', 'date', 'symbol', 
                'open', 'high', 'low', 'close', 'volume',
                'daily_return', 'ma_7', 'ma_20', 'volatility_7d', 'rsi', 'momentum',
                'volume_category', 'trend', 'rsi_signal', 'volatility_level',
                'price_change', 'price_change_pct', 'day_type'
            ]].copy()
            
            # Round numeric columns
            numeric_cols = ['open', 'high', 'low', 'close', 'daily_return', 'ma_7', 'ma_20', 
                          'volatility_7d', 'rsi', 'momentum', 'price_change', 'price_change_pct']
            output_df[numeric_cols] = output_df[numeric_cols].round(2)
            
            # Drop existing table
            with engine.connect() as conn:
                conn.execute(text('DROP TABLE IF EXISTS stock_market_analytics CASCADE'))
                conn.commit()
            
            # Load data
            output_df.to_sql(
                'stock_market_analytics',
                engine,
                if_exists='replace',
                index=False
            )
            
            # Create indexes for time-series queries
            with engine.connect() as conn:
                # Index on timestamp for time-based filtering
                conn.execute(text('CREATE INDEX IF NOT EXISTS idx_stock_timestamp ON stock_market_analytics(timestamp)'))
                # Index on symbol for stock filtering
                conn.execute(text('CREATE INDEX IF NOT EXISTS idx_stock_symbol ON stock_market_analytics(symbol)'))
                # Index on date for daily aggregations
                conn.execute(text('CREATE INDEX IF NOT EXISTS idx_stock_date ON stock_market_analytics(date)'))
                # Composite index for common queries
                conn.execute(text('CREATE INDEX IF NOT EXISTS idx_stock_symbol_date ON stock_market_analytics(symbol, date)'))
                conn.commit()
            
            logger.info(f"✅ {len(output_df)} rows loaded to stock_market_analytics")
            logger.info(f"  Symbols: {', '.join(self.stocks)}")
            logger.info(f"  Time range: {output_df['date'].min()} to {output_df['date'].max()}")
            
        except Exception as e:
            logger.error(f"❌ Database load failed: {e}")
            raise


if __name__ == "__main__":
    pipeline = StockMarketPipeline()
    pipeline.run()
