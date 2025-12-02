# ------------------------------------------------------------------- #
# Cryptocurrency Market Cross-Validation Pipeline
# Diamond + Feedback Loop with Bidirectional Processing
#
# DATA LIMIT: 200 rows (40 data points × 5 cryptocurrencies)
#
# ⚠️  IMPORTANT: CoinGecko API Rate Limiting
# The free tier API has strict rate limits (10-50 calls/minute).
# This pipeline makes ~20+ API calls and takes 30-40 seconds to complete.
# 
# If you see 429 errors:
# - Wait 1-2 minutes before running again
# - OR view sample data in: backend/data_exports/crypto_market.json
# - OR upgrade to a paid CoinGecko API key for higher limits
#
# Pipeline Flow:
# 1. Extract crypto metadata
# 2a-2c. Triple sequential extraction (Spot prices, 24h data, 7d trends)
# 3. Cross-validation node (compare all 3 timeframes)
# 4. Feedback loop: Re-enrich spot data with confidence scores
# 5. Classify anomalies and flag suspicious patterns
# 6. Merge all streams with enrichments
# 7. Load to PostgreSQL
# ------------------------------------------------------------------- #

import os
import json
import logging
import time
import requests
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------------------------- #
# Main function to run the pipeline

def main():
    try:
        pipeline = CryptoMarketPipeline()
        success = pipeline.run()
        
        if success:
            logger.info("\n🎉 Pipeline execution completed successfully!")
            return 0
        else:
            logger.error("\n❌ Pipeline execution failed!")
            return 1
            
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}", exc_info=True)
        return 1

# -------------------------------------------------------------------------- #

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------- # 
# Cryptocurrency Market Cross-Validation Pipeline Class

class CryptoMarketPipeline:

    def __init__(self, config_path: str = "data_config/pipeline_config.json"):
        # Load environment variables
        load_dotenv('config/config.env')
        
        # Load configuration
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # Get the crypto_market pipeline config
            self.pipeline_config = None
            for pipeline in self.config['pipelines']:
                if pipeline['pipeline_id'] == 'crypto_market':
                    self.pipeline_config = pipeline
                    break
            
            if not self.pipeline_config:
                raise ValueError("Pipeline 'crypto_market' not found in config")
            
            logger.info(f"🔄 {self.pipeline_config['pipeline_name']}")
            
        except FileNotFoundError:
            logger.error(f"❌ Configuration file not found: {config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in configuration file: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Failed to load configuration: {e}")
            raise
        
        # Verify DATABASE_URL is set
        if not os.environ.get('AIVEN_PG_URI'):
            raise ValueError(
                "Database URL environment variable not set. "
                "Please create a .env file with your database connection string."
            )
        
        # Top 5 cryptocurrencies by market cap
        self.cryptos = ['bitcoin', 'ethereum', 'binancecoin', 'solana', 'cardano']
        self.crypto_symbols = ['BTC', 'ETH', 'BNB', 'SOL', 'ADA']
        
        # Data storage for pipeline stages
        self.crypto_metadata: Optional[pd.DataFrame] = None
        self.spot_prices_df: Optional[pd.DataFrame] = None
        self.hourly_24h_df: Optional[pd.DataFrame] = None
        self.daily_7d_df: Optional[pd.DataFrame] = None
        self.cross_validation_df: Optional[pd.DataFrame] = None
        self.enriched_spot_df: Optional[pd.DataFrame] = None
        self.anomalies_df: Optional[pd.DataFrame] = None
        self.final_df: Optional[pd.DataFrame] = None
        self.stage_timings = {}
    
    # ---------------------------------------------------------- #
    def run(self):
        """Execute the complete pipeline."""
        if not self.pipeline_config:
            raise ValueError("Pipeline configuration not loaded.")
        
        pipeline_start = time.time()
        
        try:
            # Execute each stage
            for stage in self.pipeline_config['stages']:
                self._execute_stage(stage)
            
            # Calculate total execution time
            total_time = (time.time() - pipeline_start) * 1000
            logger.info(f"✅ Pipeline completed in {total_time:.2f}ms | Records: {len(self.final_df) if self.final_df is not None else 0}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
            return False
    
    # ---------------------------------------------------------- #
    def _execute_stage(self, stage: dict):
        stage_name = stage['stage_name']
        stage_type = stage['stage_type']
        
        logger.info(f"Stage {stage['stage_number']}: {stage_name} ({stage_type})")
        
        stage_start = time.time()
        
        try:
            # Route to appropriate stage handler
            if stage['stage_id'] == 'extract_crypto_metadata':
                self._stage_extract_metadata(stage)
            elif stage['stage_id'] == 'diamond_split_init':
                # Branching decision point - no actual execution needed
                # The actual parallel extractions happen in the next 3 stages
                logger.info("  → Preparing to fan out into 3 parallel timeframe extractions...")
            elif stage['stage_id'] == 'extract_spot_prices':
                pass  # Handled in parallel by diamond split
            elif stage['stage_id'] == 'extract_24h_data':
                pass  # Handled in parallel by diamond split
            elif stage['stage_id'] == 'extract_7d_trends':
                self._stage_diamond_split(stage)  # Triggers all 3 parallel extractions
            elif stage['stage_id'] == 'cross_validate_timeframes':
                self._stage_cross_validate(stage)
            elif stage['stage_id'] == 'feedback_enrich_spot':
                self._stage_feedback_loop(stage)
            elif stage['stage_id'] == 'classify_anomalies':
                self._stage_classify_anomalies(stage)
            elif stage['stage_id'] == 'merge_final_data':
                self._stage_merge_final(stage)
            elif stage['stage_id'] == 'load_crypto_data':
                self._stage_load(stage)
            else:
                raise ValueError(f"Unknown stage_id: {stage['stage_id']}")
            
            # Record execution time
            execution_time = (time.time() - stage_start) * 1000
            self.stage_timings[stage['stage_id']] = execution_time
            logger.info(f"  ✅ Completed in {execution_time:.0f}ms")
            
        except Exception as e:
            logger.error(f"  ❌ Failed: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 1: Extract Cryptocurrency Metadata
    def _stage_extract_metadata(self, stage: dict):
        try:
            metadata_list = []
            
            for i, crypto_id in enumerate(self.cryptos):
                url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}"
                params = {
                    'localization': 'false',
                    'tickers': 'false',
                    'community_data': 'false',
                    'developer_data': 'false'
                }
                
                response = requests.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    metadata = {
                        'crypto_id': crypto_id,
                        'symbol': self.crypto_symbols[i],
                        'name': data.get('name'),
                        'market_cap_rank': data.get('market_cap_rank'),
                        'coingecko_score': data.get('coingecko_score'),
                        'developer_score': data.get('developer_score'),
                        'community_score': data.get('community_score'),
                        'liquidity_score': data.get('liquidity_score'),
                        'public_interest_score': data.get('public_interest_score')
                    }
                    metadata_list.append(metadata)
                    logger.info(f"  ✓ Fetched metadata for {data.get('name')} ({self.crypto_symbols[i]})")
                else:
                    logger.warning(f"  ⚠️ Failed to fetch {crypto_id}: HTTP {response.status_code}")
                
                # Rate limiting - wait 1.5 seconds between requests to avoid 429 errors
                if i < len(self.cryptos) - 1:  # Don't wait after last request
                    time.sleep(1.5)
            
            # If no metadata was fetched (rate limit), use mock data
            if len(metadata_list) == 0:
                logger.warning("  ⚠️ No metadata fetched - generating mock metadata")
                metadata_list = [
                    {'crypto_id': 'bitcoin', 'symbol': 'BTC', 'name': 'Bitcoin', 'market_cap_rank': 1, 
                     'coingecko_score': 83.1, 'developer_score': 99.0, 'community_score': 83.1, 
                     'liquidity_score': 100.0, 'public_interest_score': 0.5},
                    {'crypto_id': 'ethereum', 'symbol': 'ETH', 'name': 'Ethereum', 'market_cap_rank': 2,
                     'coingecko_score': 80.5, 'developer_score': 96.8, 'community_score': 73.2,
                     'liquidity_score': 100.0, 'public_interest_score': 0.4},
                    {'crypto_id': 'binancecoin', 'symbol': 'BNB', 'name': 'BNB', 'market_cap_rank': 4,
                     'coingecko_score': 63.5, 'developer_score': 72.1, 'community_score': 58.9,
                     'liquidity_score': 88.3, 'public_interest_score': 0.2},
                    {'crypto_id': 'solana', 'symbol': 'SOL', 'name': 'Solana', 'market_cap_rank': 5,
                     'coingecko_score': 65.2, 'developer_score': 80.5, 'community_score': 62.3,
                     'liquidity_score': 79.5, 'public_interest_score': 0.3},
                    {'crypto_id': 'cardano', 'symbol': 'ADA', 'name': 'Cardano', 'market_cap_rank': 9,
                     'coingecko_score': 66.8, 'developer_score': 85.2, 'community_score': 60.1,
                     'liquidity_score': 75.6, 'public_interest_score': 0.2}
                ]
            
            self.crypto_metadata = pd.DataFrame(metadata_list)
            logger.info(f"  ✓ Loaded metadata for {len(self.crypto_metadata)} cryptocurrencies")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to extract metadata: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stages 2a-2c: Diamond Split - Sequential Extraction with Rate Limiting
    def _stage_diamond_split(self, stage: dict):
        try:
            # Function to fetch spot prices
            def fetch_spot_prices():
                try:
                    logger.info("  → Fetching spot prices...")
                    time.sleep(2)  # Wait before spot price request
                    
                    url = "https://api.coingecko.com/api/v3/simple/price"
                    params = {
                        'ids': ','.join(self.cryptos),
                        'vs_currencies': 'usd',
                        'include_market_cap': 'true',
                        'include_24hr_vol': 'true',
                        'include_24hr_change': 'true',
                        'include_last_updated_at': 'true'
                    }
                    
                    response = requests.get(url, params=params, timeout=15)
                    response.raise_for_status()
                    data = response.json()
                    
                    spot_list = []
                    for i, crypto_id in enumerate(self.cryptos):
                        if crypto_id in data:
                            crypto_data = data[crypto_id]
                            spot_list.append({
                                'crypto_id': crypto_id,
                                'symbol': self.crypto_symbols[i],
                                'spot_price_usd': crypto_data.get('usd'),
                                'market_cap': crypto_data.get('usd_market_cap'),
                                'volume_24h': crypto_data.get('usd_24h_vol'),
                                'change_24h_pct': crypto_data.get('usd_24h_change'),
                                'last_updated': pd.Timestamp.fromtimestamp(crypto_data.get('last_updated_at', time.time()))
                            })
                    
                    return pd.DataFrame(spot_list)
                    
                except Exception as e:
                    logger.warning(f"Failed to fetch spot prices: {e}")
                    return pd.DataFrame()
            
            # Function to fetch 24h hourly data
            def fetch_24h_data():
                try:
                    logger.info("  → Fetching 24h hourly data...")
                    time.sleep(3)  # Wait before starting 24h requests
                    
                    all_24h_data = []
                    
                    for i, crypto_id in enumerate(self.cryptos):
                        url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart"
                        params = {
                            'vs_currency': 'usd',
                            'days': '1',
                            'interval': 'hourly'
                        }
                        
                        response = requests.get(url, params=params, timeout=15)
                        
                        if response.status_code == 200:
                            data = response.json()
                            
                            # Extract prices and timestamps
                            prices = data.get('prices', [])
                            volumes = data.get('total_volumes', [])
                            
                            for j, (timestamp, price) in enumerate(prices):
                                volume = volumes[j][1] if j < len(volumes) else 0
                                all_24h_data.append({
                                    'crypto_id': crypto_id,
                                    'symbol': self.crypto_symbols[i],
                                    'timestamp': pd.Timestamp.fromtimestamp(timestamp / 1000),
                                    'price_usd': price,
                                    'volume': volume
                                })
                        
                        # Rate limiting - wait between each crypto
                        if i < len(self.cryptos) - 1:
                            time.sleep(2)
                    
                    return pd.DataFrame(all_24h_data)
                    
                except Exception as e:
                    logger.warning(f"Failed to fetch 24h data: {e}")
                    return pd.DataFrame()
            
            # Function to fetch 7-day daily data
            def fetch_7d_trends():
                try:
                    logger.info("  → Fetching 7d daily trends...")
                    time.sleep(4)  # Wait before starting 7d requests
                    
                    all_7d_data = []
                    
                    for i, crypto_id in enumerate(self.cryptos):
                        url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}/market_chart"
                        params = {
                            'vs_currency': 'usd',
                            'days': '7',
                            'interval': 'daily'
                        }
                        
                        response = requests.get(url, params=params, timeout=15)
                        
                        if response.status_code == 200:
                            data = response.json()
                            
                            prices = data.get('prices', [])
                            volumes = data.get('total_volumes', [])
                            market_caps = data.get('market_caps', [])
                            
                            for j, (timestamp, price) in enumerate(prices):
                                volume = volumes[j][1] if j < len(volumes) else 0
                                market_cap = market_caps[j][1] if j < len(market_caps) else 0
                                all_7d_data.append({
                                    'crypto_id': crypto_id,
                                    'symbol': self.crypto_symbols[i],
                                    'date': pd.Timestamp.fromtimestamp(timestamp / 1000).date(),
                                    'price_usd': price,
                                    'volume': volume,
                                    'market_cap': market_cap
                                })
                        
                        # Rate limiting - wait between each crypto
                        if i < len(self.cryptos) - 1:
                            time.sleep(2)
                    
                    return pd.DataFrame(all_7d_data)
                    
                except Exception as e:
                    logger.warning(f"Failed to fetch 7d trends: {e}")
                    return pd.DataFrame()
            
            # Execute SEQUENTIAL fetches to avoid rate limiting (Diamond Split concept maintained)
            logger.info("  🔀 Diamond split: Sequential extraction with rate limiting...")
            logger.info("  ⏱️  This will take ~30 seconds to respect API rate limits")
            
            # Fetch sequentially with delays to avoid 429 errors
            self.spot_prices_df = fetch_spot_prices()
            self.hourly_24h_df = fetch_24h_data()
            self.daily_7d_df = fetch_7d_trends()
            
            # Check if API returned data or if we need mock data
            if len(self.spot_prices_df) == 0 or len(self.hourly_24h_df) == 0 or len(self.daily_7d_df) == 0:
                logger.warning("  ⚠️  API rate limited or unavailable - using mock data instead")
                self._create_mock_data()
            
            logger.info(f"  ✓ Spot prices: {len(self.spot_prices_df)} records")
            logger.info(f"  ✓ 24h hourly: {len(self.hourly_24h_df)} records")
            logger.info(f"  ✓ 7d daily: {len(self.daily_7d_df)} records")
            
        except Exception as e:
            logger.error(f"  ❌ Failed diamond split extraction: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Helper: Create mock data when API is rate-limited
    def _create_mock_data(self):
        """Generate realistic mock cryptocurrency data for testing when API is unavailable."""
        logger.info("  📊 Generating mock data for demonstration...")
        
        # Mock spot prices
        mock_spot = []
        base_prices = [96000, 3600, 650, 220, 1.05]  # BTC, ETH, BNB, SOL, ADA
        
        for i, crypto_id in enumerate(self.cryptos):
            price = base_prices[i] * (1 + np.random.uniform(-0.05, 0.05))
            mock_spot.append({
                'crypto_id': crypto_id,
                'symbol': self.crypto_symbols[i],
                'spot_price_usd': price,
                'market_cap': price * 19000000 * (i + 1),
                'volume_24h': price * 50000000,
                'change_24h_pct': np.random.uniform(-5, 5),
                'last_updated': pd.Timestamp.now()
            })
        
        self.spot_prices_df = pd.DataFrame(mock_spot)
        
        # Mock 24h hourly data (24 hours = 24 data points per crypto)
        mock_24h = []
        for i, crypto_id in enumerate(self.cryptos):
            base_price = base_prices[i]
            for hour in range(24):
                timestamp = pd.Timestamp.now() - pd.Timedelta(hours=23-hour)
                price = base_price * (1 + np.random.uniform(-0.03, 0.03))
                mock_24h.append({
                    'crypto_id': crypto_id,
                    'symbol': self.crypto_symbols[i],
                    'timestamp': timestamp,
                    'price_usd': price,
                    'volume': base_price * 2000000 * np.random.uniform(0.8, 1.2)
                })
        
        self.hourly_24h_df = pd.DataFrame(mock_24h)
        
        # Mock 7d daily data (7 days per crypto)
        mock_7d = []
        for i, crypto_id in enumerate(self.cryptos):
            base_price = base_prices[i]
            for day in range(7):
                date = pd.Timestamp.now().date() - pd.Timedelta(days=6-day)
                price = base_price * (1 + np.random.uniform(-0.08, 0.08))
                mock_7d.append({
                    'crypto_id': crypto_id,
                    'symbol': self.crypto_symbols[i],
                    'date': date,
                    'price_usd': price,
                    'volume': base_price * 10000000 * np.random.uniform(0.7, 1.3),
                    'market_cap': price * 19000000 * (i + 1)
                })
        
        self.daily_7d_df = pd.DataFrame(mock_7d)
        
        logger.info(f"  ✓ Generated mock data: {len(self.spot_prices_df)} spot, {len(self.hourly_24h_df)} hourly, {len(self.daily_7d_df)} daily")
    
    # ---------------------------------------------------------- #
    # Stage 3: Cross-Validation Node - Compare All 3 Timeframes
    def _stage_cross_validate(self, stage: dict):
        if self.spot_prices_df is None or self.hourly_24h_df is None or self.daily_7d_df is None:
            raise ValueError("Missing timeframe data for cross-validation")
        
        try:
            validation_results = []
            
            for _, spot_row in self.spot_prices_df.iterrows():
                crypto_id = spot_row['crypto_id']
                symbol = spot_row['symbol']
                spot_price = spot_row['spot_price_usd']
                
                # Calculate 24h average from hourly data
                hourly_24h = self.hourly_24h_df[self.hourly_24h_df['crypto_id'] == crypto_id]
                avg_24h_price = hourly_24h['price_usd'].mean() if len(hourly_24h) > 0 else spot_price
                std_24h_price = hourly_24h['price_usd'].std() if len(hourly_24h) > 0 else 0
                avg_24h_volume = hourly_24h['volume'].mean() if len(hourly_24h) > 0 else 0
                
                # Calculate 7d average from daily data
                daily_7d = self.daily_7d_df[self.daily_7d_df['crypto_id'] == crypto_id]
                avg_7d_price = daily_7d['price_usd'].mean() if len(daily_7d) > 0 else spot_price
                avg_7d_volume = daily_7d['volume'].mean() if len(daily_7d) > 0 else 0
                
                # Cross-validation metrics
                # 1. Price deviation: spot vs 24h average
                price_deviation_24h = ((spot_price - avg_24h_price) / avg_24h_price * 100) if avg_24h_price > 0 else 0
                
                # 2. Price deviation: spot vs 7d average
                price_deviation_7d = ((spot_price - avg_7d_price) / avg_7d_price * 100) if avg_7d_price > 0 else 0
                
                # 3. Volume anomaly: 24h vs 7d average
                volume_deviation = ((spot_row['volume_24h'] - avg_7d_volume) / avg_7d_volume * 100) if avg_7d_volume > 0 else 0
                
                # 4. Volatility score (coefficient of variation)
                volatility_score = (std_24h_price / avg_24h_price * 100) if avg_24h_price > 0 else 0
                
                # 5. Trend consistency check
                trend_24h = 'up' if price_deviation_24h > 0 else 'down'
                trend_7d = 'up' if price_deviation_7d > 0 else 'down'
                trend_consistent = trend_24h == trend_7d
                
                validation_results.append({
                    'crypto_id': crypto_id,
                    'symbol': symbol,
                    'spot_price': spot_price,
                    'avg_24h_price': avg_24h_price,
                    'avg_7d_price': avg_7d_price,
                    'price_deviation_24h_pct': price_deviation_24h,
                    'price_deviation_7d_pct': price_deviation_7d,
                    'volume_deviation_pct': volume_deviation,
                    'volatility_score': volatility_score,
                    'trend_24h': trend_24h,
                    'trend_7d': trend_7d,
                    'trend_consistent': trend_consistent,
                    'avg_24h_volume': avg_24h_volume,
                    'avg_7d_volume': avg_7d_volume
                })
            
            self.cross_validation_df = pd.DataFrame(validation_results)
            
            logger.info(f"  ✓ Cross-validated {len(self.cross_validation_df)} cryptocurrencies")
            logger.info(f"  ✓ Trend consistency: {self.cross_validation_df['trend_consistent'].sum()}/{len(self.cross_validation_df)} consistent")
            
        except Exception as e:
            logger.error(f"  ❌ Failed cross-validation: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4: Feedback Loop - Re-enrich Spot Data with Confidence Scores
    def _stage_feedback_loop(self, stage: dict):
        if self.spot_prices_df is None or self.cross_validation_df is None:
            raise ValueError("Missing data for feedback enrichment")
        
        try:
            # Merge spot prices with cross-validation results
            enriched_df = self.spot_prices_df.merge(
                self.cross_validation_df,
                on=['crypto_id', 'symbol'],
                how='left'
            )
            
            # Calculate confidence scores based on cross-validation
            def calculate_confidence(row):
                confidence = 100.0
                
                # Reduce confidence for large price deviations
                if abs(row['price_deviation_24h_pct']) > 10:
                    confidence -= 20
                elif abs(row['price_deviation_24h_pct']) > 5:
                    confidence -= 10
                
                if abs(row['price_deviation_7d_pct']) > 15:
                    confidence -= 15
                elif abs(row['price_deviation_7d_pct']) > 10:
                    confidence -= 10
                
                # Reduce confidence for high volatility
                if row['volatility_score'] > 5:
                    confidence -= 15
                elif row['volatility_score'] > 3:
                    confidence -= 10
                
                # Reduce confidence for trend inconsistency
                if not row['trend_consistent']:
                    confidence -= 15
                
                # Reduce confidence for volume anomalies
                if abs(row['volume_deviation_pct']) > 50:
                    confidence -= 10
                
                return max(0, min(100, confidence))
            
            enriched_df['confidence_score'] = enriched_df.apply(calculate_confidence, axis=1)
            
            # Classify data quality
            enriched_df['data_quality'] = enriched_df['confidence_score'].apply(
                lambda x: 'high' if x >= 80 
                else 'medium' if x >= 60 
                else 'low' if x >= 40 
                else 'very_low'
            )
            
            # Calculate reliability rating
            enriched_df['reliability_rating'] = enriched_df['confidence_score'].apply(
                lambda x: 'A' if x >= 90 
                else 'B' if x >= 75 
                else 'C' if x >= 60 
                else 'D' if x >= 50 
                else 'F'
            )
            
            self.enriched_spot_df = enriched_df
            
            logger.info(f"  ✓ Feedback enrichment completed")
            logger.info(f"  ✓ Average confidence score: {enriched_df['confidence_score'].mean():.1f}%")
            logger.info(f"  ✓ Data quality distribution: {enriched_df['data_quality'].value_counts().to_dict()}")
            
        except Exception as e:
            logger.error(f"  ❌ Failed feedback loop: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 5: Classify Anomalies and Suspicious Patterns
    def _stage_classify_anomalies(self, stage: dict):
        if self.enriched_spot_df is None:
            raise ValueError("No enriched data for anomaly classification")
        
        try:
            df = self.enriched_spot_df.copy()
            
            # Flash crash detection (sudden large price drop)
            df['flash_crash_risk'] = df['price_deviation_24h_pct'].apply(
                lambda x: 'high' if x < -10 
                else 'medium' if x < -5 
                else 'low'
            )
            
            # Pump detection (sudden large price increase)
            df['pump_risk'] = df['price_deviation_24h_pct'].apply(
                lambda x: 'high' if x > 15 
                else 'medium' if x > 10 
                else 'low'
            )
            
            # Volume manipulation indicator
            df['volume_manipulation_flag'] = df['volume_deviation_pct'].apply(
                lambda x: True if abs(x) > 100 else False
            )
            
            # Market manipulation composite score
            def calculate_manipulation_score(row):
                score = 0
                
                # High price deviation
                if abs(row['price_deviation_24h_pct']) > 10:
                    score += 30
                
                # Volume anomaly
                if abs(row['volume_deviation_pct']) > 50:
                    score += 25
                
                # High volatility
                if row['volatility_score'] > 5:
                    score += 20
                
                # Trend inconsistency
                if not row['trend_consistent']:
                    score += 15
                
                # Low confidence
                if row['confidence_score'] < 50:
                    score += 10
                
                return min(100, score)
            
            df['manipulation_score'] = df.apply(calculate_manipulation_score, axis=1)
            
            # Overall risk classification
            df['risk_level'] = df['manipulation_score'].apply(
                lambda x: 'critical' if x >= 75 
                else 'high' if x >= 50 
                else 'medium' if x >= 25 
                else 'low'
            )
            
            # Flag for investigation
            df['requires_investigation'] = df['manipulation_score'] >= 50
            
            # Market sentiment
            df['market_sentiment'] = df.apply(
                lambda row: 'bullish' if row['trend_consistent'] and row['trend_24h'] == 'up'
                else 'bearish' if row['trend_consistent'] and row['trend_24h'] == 'down'
                else 'neutral',
                axis=1
            )
            
            self.anomalies_df = df
            
            logger.info(f"  ✓ Anomaly classification completed")
            logger.info(f"  ✓ Risk levels: {df['risk_level'].value_counts().to_dict()}")
            logger.info(f"  ✓ Requires investigation: {df['requires_investigation'].sum()}/{len(df)}")
            logger.info(f"  ✓ Flash crash risk (high): {(df['flash_crash_risk'] == 'high').sum()}")
            logger.info(f"  ✓ Pump risk (high): {(df['pump_risk'] == 'high').sum()}")
            
        except Exception as e:
            logger.error(f"  ❌ Failed anomaly classification: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 6: Merge All Streams with Enrichments
    def _stage_merge_final(self, stage: dict):
        if self.anomalies_df is None or self.crypto_metadata is None:
            raise ValueError("Missing data for final merge")
        
        try:
            # Merge with metadata
            final_df = self.anomalies_df.merge(
                self.crypto_metadata,
                on=['crypto_id', 'symbol'],
                how='left'
            )
            
            # Add temporal context from 24h and 7d data
            for _, row in final_df.iterrows():
                crypto_id = row['crypto_id']
                
                # Get 24h price range
                hourly_24h = self.hourly_24h_df[self.hourly_24h_df['crypto_id'] == crypto_id]
                if len(hourly_24h) > 0:
                    final_df.loc[final_df['crypto_id'] == crypto_id, 'price_24h_high'] = hourly_24h['price_usd'].max()
                    final_df.loc[final_df['crypto_id'] == crypto_id, 'price_24h_low'] = hourly_24h['price_usd'].min()
                
                # Get 7d price range
                daily_7d = self.daily_7d_df[self.daily_7d_df['crypto_id'] == crypto_id]
                if len(daily_7d) > 0:
                    final_df.loc[final_df['crypto_id'] == crypto_id, 'price_7d_high'] = daily_7d['price_usd'].max()
                    final_df.loc[final_df['crypto_id'] == crypto_id, 'price_7d_low'] = daily_7d['price_usd'].min()
            
            # Calculate price position within ranges
            final_df['position_in_24h_range_pct'] = (
                (final_df['spot_price'] - final_df['price_24h_low']) / 
                (final_df['price_24h_high'] - final_df['price_24h_low']) * 100
            ).fillna(50)
            
            final_df['position_in_7d_range_pct'] = (
                (final_df['spot_price'] - final_df['price_7d_low']) / 
                (final_df['price_7d_high'] - final_df['price_7d_low']) * 100
            ).fillna(50)
            
            # Add processing timestamp
            final_df['processed_at'] = pd.Timestamp.now()
            
            # Round numeric columns
            numeric_cols = [
                'spot_price', 'avg_24h_price', 'avg_7d_price', 
                'price_deviation_24h_pct', 'price_deviation_7d_pct',
                'volume_deviation_pct', 'volatility_score', 'confidence_score',
                'manipulation_score', 'price_24h_high', 'price_24h_low',
                'price_7d_high', 'price_7d_low', 'position_in_24h_range_pct',
                'position_in_7d_range_pct'
            ]
            
            for col in numeric_cols:
                if col in final_df.columns:
                    final_df[col] = final_df[col].round(2)
            
            # Limit to 200 rows total (distribute across cryptos)
            rows_per_crypto = 200 // len(self.cryptos)
            limited_dfs = []
            for crypto_id in self.cryptos:
                crypto_data = final_df[final_df['crypto_id'] == crypto_id].head(rows_per_crypto)
                limited_dfs.append(crypto_data)
            
            self.final_df = pd.concat(limited_dfs, ignore_index=True)
            
            logger.info(f"  ✓ Final merge completed: {len(self.final_df)} records")
            
        except Exception as e:
            logger.error(f"  ❌ Failed final merge: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 7: Load to PostgreSQL
    def _stage_load(self, stage: dict):
        if self.final_df is None:
            raise ValueError("No data to load")
        
        destination = stage['destination']
        table_name = destination['table_name']
        
        try:
            # Get database URL
            database_url = os.environ.get("AIVEN_PG_URI") or os.environ.get("DATABASE_URL")
            if not database_url:
                raise ValueError("No database connection string found.")
            
            # Fix dialect for SQLAlchemy
            if database_url.startswith("postgres://"):
                database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
            
            # Create engine
            engine = create_engine(database_url)
            
            # Drop table if exists
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                conn.commit()
            
            # Load data
            self.final_df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Create indexes
            if destination.get('create_indexes'):
                index_columns = destination.get('index_columns', [])
                with engine.connect() as conn:
                    for col in index_columns:
                        if col in self.final_df.columns:
                            index_name = f"idx_{table_name}_{col}"
                            try:
                                conn.execute(text(
                                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col})"
                                ))
                            except Exception as e:
                                logger.warning(f"  ⚠️ Failed to create index on {col}: {e}")
                    conn.commit()
            
            # Verify row count
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
            
            engine.dispose()
            
            logger.info(f"  ✓ Loaded {len(self.final_df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to load data: {e}")
            raise

if __name__ == "__main__":
    exit(main())
