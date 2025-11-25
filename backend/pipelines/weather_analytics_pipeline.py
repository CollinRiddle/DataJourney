# ------------------------------------------------------------------- #
# Multi-Region Weather Analytics Pipeline
# Fan-out/Fan-in ETL with Parallel Processing
#
# DATA LIMIT: 200 rows (sampled from hourly data)
#
# Pipeline Flow:
# 1. Initialize parallel fetch configuration
# 2-4. Fan-out: Fetch 3 regions in parallel (North America, Europe, Asia)
# 5. Fan-in: Merge all regional data
# 6. Transform: Calculate weather analytics
# 7. Load to PostgreSQL
# ------------------------------------------------------------------- #

import os
import json
import logging
import time
import requests
from typing import Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------------------------- #
# Main function to run the pipeline

def main():
    try:
        pipeline = WeatherPipeline()
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
# Weather Analytics Pipeline Class

class WeatherPipeline:

    def __init__(self, config_path: str = "backend/data_config/pipeline_config.json"):
        # Load environment variables
        load_dotenv()
        
        # Load configuration
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # Get the weather_analytics pipeline config
            self.pipeline_config = None
            for pipeline in self.config['pipelines']:
                if pipeline['pipeline_id'] == 'weather_analytics':
                    self.pipeline_config = pipeline
                    break
            
            if not self.pipeline_config:
                raise ValueError("Pipeline 'weather_analytics' not found in config")
            
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
        
        self.regional_dfs = {}
        self.merged_df: Optional[pd.DataFrame] = None
        self.final_df: Optional[pd.DataFrame] = None
        self.stage_timings = {}
        self.regions_config = [
            {'name': 'North America', 'city': 'New York', 'lat': 40.7128, 'lon': -74.0060},
            {'name': 'Europe', 'city': 'London', 'lat': 51.5074, 'lon': -0.1278},
            {'name': 'Asia', 'city': 'Tokyo', 'lat': 35.6762, 'lon': 139.6503}
        ]
    
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
            if stage['stage_id'] == 'initiate_parallel_fetch':
                self._stage_init_parallel(stage)
            elif stage['stage_id'] == 'fetch_north_america':
                pass  # Handled in parallel by init stage
            elif stage['stage_id'] == 'fetch_europe':
                pass  # Handled in parallel by init stage
            elif stage['stage_id'] == 'fetch_asia':
                pass  # Handled in parallel by init stage
            elif stage['stage_id'] == 'merge_regional_data':
                self._stage_merge(stage)
            elif stage['stage_id'] == 'transform_weather_data':
                self._stage_transform(stage)
            elif stage['stage_id'] == 'load_weather_data':
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
    # Stage 1: Initialize and execute parallel fetch (Fan-out)
    def _stage_init_parallel(self, stage: dict):
        try:
            # Function to fetch data for a single region
            def fetch_region_data(region):
                try:
                    # Calculate date range (last 92 days for hourly data, will sample to 200 rows)
                    url = (
                        f"https://api.open-meteo.com/v1/forecast?"
                        f"latitude={region['lat']}&longitude={region['lon']}&"
                        f"hourly=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m&"
                        f"past_days=92"
                    )
                    
                    response = requests.get(url, timeout=15)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Create DataFrame
                    df = pd.DataFrame(data['hourly'])
                    df['region'] = region['name']
                    df['city'] = region['city']
                    
                    return region['name'], df
                    
                except Exception as e:
                    logger.warning(f"Failed to fetch {region['name']}: {e}")
                    return region['name'], None
            
            # Execute parallel fetches using ThreadPoolExecutor (Fan-out)
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(fetch_region_data, region): region for region in self.regions_config}
                
                for future in as_completed(futures):
                    region_name, df = future.result()
                    if df is not None:
                        self.regional_dfs[region_name] = df
            
            logger.info(f"  ✓ Fetched data from {len(self.regional_dfs)} regions in parallel")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to fetch regional data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 5: Merge regional data (Fan-in)
    def _stage_merge(self, stage: dict):
        if not self.regional_dfs:
            raise ValueError("No regional data to merge")
        
        try:
            # Fan-in: Concatenate all regional dataframes
            dfs_to_merge = list(self.regional_dfs.values())
            self.merged_df = pd.concat(dfs_to_merge, ignore_index=True)
            
            # Standardize timestamps
            self.merged_df['time'] = pd.to_datetime(self.merged_df['time'])
            self.merged_df['date'] = self.merged_df['time'].dt.date
            self.merged_df['hour'] = self.merged_df['time'].dt.hour
            
            # Limit to 200 rows (sample evenly across time)
            if len(self.merged_df) > 200:
                # Sample every nth row to get approximately 200 rows
                step = len(self.merged_df) // 200
                self.merged_df = self.merged_df.iloc[::step].head(200).reset_index(drop=True)
            
            logger.info(f"  ✓ Merged {len(self.regional_dfs)} regions into {len(self.merged_df)} records")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to merge data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 6: Transform and calculate analytics
    def _stage_transform(self, stage: dict):
        if self.merged_df is None:
            raise ValueError("No merged data to transform")
        
        try:
            df = self.merged_df.copy()
            
            # Weather classification
            df['weather_type'] = df.apply(lambda row:
                'rainy' if row['precipitation'] > 5
                else 'hot' if row['temperature_2m'] > 25
                else 'cold' if row['temperature_2m'] < 5
                else 'moderate',
                axis=1
            )
            
            # Calculate comfort index (simplified)
            df['comfort_index'] = df.apply(lambda row:
                100 - abs(row['temperature_2m'] - 22) * 2 - row['relative_humidity_2m'] * 0.3,
                axis=1
            )
            
            # Wind category
            df['wind_category'] = pd.cut(
                df['wind_speed_10m'],
                bins=[0, 5, 10, 20, 100],
                labels=['calm', 'light', 'moderate', 'strong']
            )
            
            # Add processing timestamp
            df['processed_at'] = pd.Timestamp.now()
            
            self.final_df = df
            
            logger.info(f"  ✓ Applied transformations to {len(self.final_df)} records")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to transform data: {e}")
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
