# ------------------------------------------------------------------- #
# SpaceX Launch Analytics Pipeline
# Advanced ETL with Multi-Source Integration and Conditional Branching
#
# DATA LIMIT: 200 launches (to conserve space)
#
# Pipeline Flow:
# 1. Extract from SpaceX API (launches, rockets, launchpads)
# 2. Data Quality Assessment & Branching
# 3. Branch A: Complete Data → Full Analytics
# 4. Branch B: Incomplete Data → Basic Processing
# 5. Merge & Enrich with Statistical Metrics
# 6. Load to PostgreSQL
# ------------------------------------------------------------------- #

import os
import json
import logging
import time
import requests
from typing import Optional, List, Dict
from datetime import datetime

import pandas as pd
from pandas import Series
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------------------------- #
# Main function to run the pipeline

def main():
    try:
        pipeline = SpaceXPipeline()
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
# SpaceX Launch Analytics Pipeline Class

class SpaceXPipeline:

    def __init__(self, config_path: str = "backend/data_config/pipeline_config.json"):
        # Load environment variables
        load_dotenv()
        
        # Load configuration
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # Get the spacex_launches pipeline config
            self.pipeline_config = None
            for pipeline in self.config['pipelines']:
                if pipeline['pipeline_id'] == 'spacex_launches':
                    self.pipeline_config = pipeline
                    break
            
            if not self.pipeline_config:
                raise ValueError("Pipeline 'spacex_launches' not found in config")
            
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
        
        self.launches_df: Optional[pd.DataFrame] = None
        self.rockets_df: Optional[pd.DataFrame] = None
        self.launchpads_df: Optional[pd.DataFrame] = None
        self.complete_data_df: Optional[pd.DataFrame] = None
        self.incomplete_data_df: Optional[pd.DataFrame] = None
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
            if stage['stage_id'] == 'extract_spacex_data':
                self._stage_extract(stage)
            elif stage['stage_id'] == 'enrich_and_join':
                self._stage_enrich(stage)
            elif stage['stage_id'] == 'data_quality_branch':
                self._stage_quality_branch(stage)
            elif stage['stage_id'] == 'process_complete_data':
                self._stage_process_complete(stage)
            elif stage['stage_id'] == 'process_incomplete_data':
                self._stage_process_incomplete(stage)
            elif stage['stage_id'] == 'merge_enrich_load':
                self._stage_merge_and_load(stage)
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
    # Stage 1: Extract from Multiple SpaceX API Endpoints
    def _stage_extract(self, stage: dict):
        source = stage['source']
        base_url = source['base_url']
        
        try:
            # Fetch ALL launches, then filter to last 200
            launches_url = f"{base_url}/launches"
            response = requests.get(launches_url, timeout=15)
            response.raise_for_status()
            launches_data = response.json()
            
            # Sort by date and take last 200 launches (most recent)
            launches_data = sorted(
                launches_data, 
                key=lambda x: x.get('date_unix', 0)
            )[-200:]
            
            logger.info(f"Processing {len(launches_data)} most recent launches...")
            
            launches_list = []
            for launch in launches_data:
                try:
                    launch_obj = {
                        'flight_number': launch.get('flight_number'),
                        'name': launch.get('name'),
                        'date_utc': launch.get('date_utc'),
                        'date_unix': launch.get('date_unix'),
                        'success': launch.get('success'),
                        'failures': json.dumps(launch.get('failures', [])),
                        'details': launch.get('details'),
                        'rocket_id': launch.get('rocket'),
                        'launchpad_id': launch.get('launchpad'),
                        'crew': len(launch.get('crew', [])),
                        'payloads': len(launch.get('payloads', [])),
                        'cores_used': len(launch.get('cores', [])),
                    }
                    launches_list.append(launch_obj)
                except Exception as e:
                    logger.warning(f"Failed to parse launch {launch.get('name', 'unknown')}: {e}")
                    continue
            
            self.launches_df = pd.DataFrame(launches_list)
            logger.info(f"  ✓ Loaded {len(self.launches_df)} launches")
            
            # Create mock rocket data from launches
            rocket_ids = set(launches_data[i].get('rocket') for i in range(len(launches_data)) if launches_data[i].get('rocket'))
            rockets_list = []
            for rocket_id in sorted(rocket_ids):
                try:
                    rocket_v5 = f"{base_url}/rockets/{rocket_id}"
                    rocket_v4 = rocket_v5.replace("/v5/", "/v4/")

                    rocket = self._safe_fetch(rocket_v5, rocket_v4, item_type=f"rocket {rocket_id}")
                    if rocket is None:
                        continue
                    
                    rocket_obj = {
                        'rocket_id': rocket.get('id'),
                        'rocket_name': rocket.get('name'),
                        'rocket_type': rocket.get('type'),
                        'active': rocket.get('active'),
                        'stages': rocket.get('stages'),
                        'boosters': rocket.get('boosters', 0),
                        'cost_per_launch': rocket.get('cost_per_launch'),
                        'success_rate': rocket.get('success_rate_pct'),
                        'first_flight': rocket.get('first_flight'),
                        'country': rocket.get('country'),
                        'company': rocket.get('company'),
                        'height_meters': rocket.get('height', {}).get('meters') if isinstance(rocket.get('height'), dict) else None,
                        'diameter_meters': rocket.get('diameter', {}).get('meters') if isinstance(rocket.get('diameter'), dict) else None,
                        'mass_kg': rocket.get('mass', {}).get('kg') if isinstance(rocket.get('mass'), dict) else None,
                    }
                    rockets_list.append(rocket_obj)
                except Exception as e:
                    logger.warning(f"Failed to fetch rocket {rocket_id}: {e}")
                    continue
            
            self.rockets_df = pd.DataFrame(rockets_list)
            logger.info(f"  ✓ Loaded {len(self.rockets_df)} rockets")
            
            # Fetch launchpads
            launchpads_v5 = f"{base_url}/launchpads"
            launchpads_v4 = launchpads_v5.replace("/v5/", "/v4/")

            launchpads_data = self._safe_fetch(launchpads_v5, launchpads_v4, item_type="launchpads")
            if launchpads_data is None:
                logger.warning("⚠️ No launchpad data available — entering partial mode.")
                self.launchpads_df = pd.DataFrame()
                return

            
            launchpads_list = []
            for pad in launchpads_data:
                try:
                    pad_obj = {
                        'launchpad_id': pad.get('id'),
                        'launchpad_name': pad.get('name'),
                        'launchpad_full_name': pad.get('full_name'),
                        'locality': pad.get('locality'),
                        'region': pad.get('region'),
                        'latitude': pad.get('latitude'),
                        'longitude': pad.get('longitude'),
                        'launch_attempts': pad.get('launch_attempts'),
                        'launch_successes': pad.get('launch_successes'),
                        'status': pad.get('status'),
                    }
                    launchpads_list.append(pad_obj)
                except Exception as e:
                    logger.warning(f"Failed to parse launchpad {pad.get('name', 'unknown')}: {e}")
                    continue
            
            self.launchpads_df = pd.DataFrame(launchpads_list)
            logger.info(f"  ✓ Loaded {len(self.launchpads_df)} launchpads")
            
        except requests.RequestException as e:
            logger.error(f"  ❌ Failed to fetch data from SpaceX API: {e}")
            raise
        except Exception as e:
            logger.error(f"  ❌ Failed to extract data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 2: Enrich and Join Data
    def _stage_enrich(self, stage: dict):

        if self.launches_df is None:
            raise ValueError("Launch data missing — cannot proceed.")

        # If rockets or pads are missing, fallback to empty dataframes
        if self.rockets_df is None:
            logger.warning("⚠️ No rocket data — enrichment will be partial.")
            self.rockets_df = pd.DataFrame(columns=['rocket_id'])

        if self.launchpads_df is None:
            logger.warning("⚠️ No launchpad data — enrichment will be partial.")
            self.launchpads_df = pd.DataFrame(columns=['launchpad_id'])

        try:
            # Join launches with rockets
            enriched_df = self.launches_df.merge(
                self.rockets_df,
                on='rocket_id',
                how='left'
            )
            
            # Join with launchpads
            enriched_df = enriched_df.merge(
                self.launchpads_df,
                on='launchpad_id',
                how='left'
            )
            
            # Convert date fields
            enriched_df['date_utc'] = pd.to_datetime(
                enriched_df['date_utc'], 
                errors='coerce',
                utc=True                  # always load as UTC tz-aware
            ).dt.tz_convert(None)         # strip timezone → tz-naive

            enriched_df['launch_year'] = enriched_df['date_utc'].dt.year
            enriched_df['launch_month'] = enriched_df['date_utc'].dt.month
            enriched_df['launch_day_of_week'] = enriched_df['date_utc'].dt.day_name()
            
            # Calculate launch cost efficiency (with safe division)
            enriched_df['cost_per_payload'] = None
            mask = (enriched_df['payloads'] > 0) & (enriched_df['cost_per_launch'].notna())
            enriched_df.loc[mask, 'cost_per_payload'] = (
                enriched_df.loc[mask, 'cost_per_launch'] / enriched_df.loc[mask, 'payloads']
            )
            
            self.launches_df = enriched_df
            
            logger.info(f"  ✓ Enriched {len(self.launches_df)} launch records")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to enrich data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 3: Data Quality Assessment and Branching
    def _stage_quality_branch(self, stage: dict):
        if self.launches_df is None:
            raise ValueError("No launch data to assess.")
        
        try:
            # Define completeness criteria
            required_fields = ['success', 'rocket_name', 'launchpad_name', 
                             'cost_per_launch', 'details']
            
            # Calculate completeness score for each record
            completeness_scores = []
            for _, row in self.launches_df.iterrows():
                non_null_count = sum(1 for field in required_fields if pd.notna(row.get(field)))
                completeness_scores.append(non_null_count / len(required_fields))
            
            self.launches_df['completeness_score'] = completeness_scores
            
            # Branch based on completeness (>= 80% complete)
            complete_mask = self.launches_df['completeness_score'] >= 0.8
            
            self.complete_data_df = self.launches_df[complete_mask].copy()
            self.incomplete_data_df = self.launches_df[~complete_mask].copy()
            
            logger.info(f"  ✓ Branched into {len(self.complete_data_df)} complete + {len(self.incomplete_data_df)} incomplete")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to branch data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4a: Process Complete Data with Advanced Analytics
    def _stage_process_complete(self, stage: dict):
        if self.complete_data_df is None or len(self.complete_data_df) == 0:
            logger.warning("No complete data to process")
            self.complete_data_df = pd.DataFrame()
            return
        
        try:
            df = self.complete_data_df.copy()
            
            # Calculate success metrics
            df['mission_outcome'] = df['success'].apply(
                lambda x: 'Success' if x == True else 'Failure' if x == False else 'Unknown'
            )
            
            # Calculate rocket reliability score (with safe defaults)
            def calc_reliability(row):
                try:
                    if pd.notna(row.get('success_rate')):
                        return (row['success_rate'] / 100) * row['completeness_score']
                    else:
                        return row['completeness_score']
                except:
                    return row['completeness_score']
            
            df['reliability_score'] = df.apply(calc_reliability, axis=1)
            
            # Determine launch complexity
            df['mission_complexity'] = df.apply(
                lambda row: 'High' if row.get('crew', 0) > 0 or row.get('payloads', 0) > 3
                else 'Medium' if row.get('payloads', 0) > 1
                else 'Low',
                axis=1
            )
            
            # Calculate days since launch
            df['days_since_launch'] = df['date_utc'].apply(
                lambda x: (pd.Timestamp.now() - x).days if pd.notnull(x) else None
            )
            
            # Add data processing tier
            df['processing_tier'] = 'complete_analytics'
            
            self.complete_data_df = df
            
            logger.info(f"  ✓ Processed {len(self.complete_data_df)} complete records")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to process complete data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4b: Process Incomplete Data with Basic Metrics
    def _stage_process_incomplete(self, stage: dict):
        if self.incomplete_data_df is None or len(self.incomplete_data_df) == 0:
            logger.warning("No incomplete data to process")
            self.incomplete_data_df = pd.DataFrame()
            return
        
        try:
            df = self.incomplete_data_df.copy()
            
            # Basic success classification
            df['mission_outcome'] = df['success'].apply(
                lambda x: 'Success' if x == True else 'Failure' if x == False else 'Unknown'
            )
            
            # Simplified reliability (based only on completeness)
            df['reliability_score'] = df['completeness_score']
            
            # Simple complexity classification
            df['mission_complexity'] = 'Unknown'
            
            # Calculate days since launch (if date available)
            if 'date_utc' in df.columns:
                df['days_since_launch'] = (pd.Timestamp.now() - pd.to_datetime(df['date_utc'], errors='coerce')).dt.days
            else:
                df['days_since_launch'] = None
            
            # Add data processing tier
            df['processing_tier'] = 'basic_metrics'
            
            self.incomplete_data_df = df
            
            logger.info(f"  ✓ Processed {len(self.incomplete_data_df)} incomplete records")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to process incomplete data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 5: Merge Branches, Add Final Enrichments, and Load
    def _stage_merge_and_load(self, stage: dict):
        try:
            # Merge both processing branches
            dfs_to_merge = []
            if self.complete_data_df is not None and len(self.complete_data_df) > 0:
                dfs_to_merge.append(self.complete_data_df)
            if self.incomplete_data_df is not None and len(self.incomplete_data_df) > 0:
                dfs_to_merge.append(self.incomplete_data_df)
            
            if not dfs_to_merge:
                raise ValueError("No data available to merge")
            
            self.final_df = pd.concat(dfs_to_merge, ignore_index=True)
            
            # Add final enrichments
            # Calculate aggregate statistics (with safe groupby)
            try:
                self.final_df['avg_success_rate_by_rocket'] = self.final_df.groupby('rocket_name')['success'].transform('mean')
                self.final_df['launches_by_rocket'] = self.final_df.groupby('rocket_name')['rocket_name'].transform('count')
                self.final_df['avg_success_rate_by_pad'] = self.final_df.groupby('launchpad_name')['success'].transform('mean')
            except Exception as e:
                logger.warning(f"Failed to calculate aggregate stats: {e}")
                self.final_df['avg_success_rate_by_rocket'] = None
                self.final_df['launches_by_rocket'] = None
                self.final_df['avg_success_rate_by_pad'] = None
            
            # Add timestamp
            self.final_df['processed_at'] = pd.Timestamp.now()
            
            # Load to database
            destination = stage['destination']
            table_name = destination['table_name']
            
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
            
            logger.info(f"  ✓ Merged & loaded {len(self.final_df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"  ❌ Failed to merge and load data: {e}")
            raise


    # ---------------------------------------------------------- #
    # Safe fetch with fallback and non-fatal handling
    def _safe_fetch(self, url_v5: str, url_v4: str = None, item_type: str = "resource"):
        """
        Attempts to fetch data from a v5 endpoint, falls back to v4,
        and returns None instead of raising fatal errors.
        """
        try:
            response = requests.get(url_v5, timeout=15)
            if response.status_code == 404 and url_v4:
                logger.warning(f"⚠️ {item_type} missing at v5, retrying v4...")
                response = requests.get(url_v4, timeout=15)

            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.warning(f"⚠️ Skipping {item_type}: {e}")
            return None
    
# ---------------------------------------------------------- #

if __name__ == "__main__":
    exit(main())