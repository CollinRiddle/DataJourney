# ------------------------------------------------------------------- #
# SpaceX Launch Analytics Pipeline
# Advanced ETL with Multi-Source Integration and Conditional Branching
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
        logger.info("Initializing SpaceX Launch Analytics Pipeline...")
        
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
            
            logger.info(f"✅ Configuration loaded: {self.pipeline_config['pipeline_name']}")
            
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
        logger.info("="*60)
        if not self.pipeline_config:
            raise ValueError("Pipeline configuration not loaded.")
        logger.info(f"Starting Pipeline: {self.pipeline_config['pipeline_name']}")
        logger.info("="*60)
        
        pipeline_start = time.time()
        
        try:
            # Execute each stage
            for stage in self.pipeline_config['stages']:
                self._execute_stage(stage)
            
            # Calculate total execution time
            total_time = (time.time() - pipeline_start) * 1000
            logger.info("="*60)
            logger.info(f"✅ Pipeline completed successfully in {total_time:.2f}ms")
            logger.info(f"Final record count: {len(self.final_df) if self.final_df is not None else 0}")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
            return False
    
    # ---------------------------------------------------------- #
    def _execute_stage(self, stage: dict):
        stage_name = stage['stage_name']
        stage_type = stage['stage_type']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Stage {stage['stage_number']}: {stage_name}")
        logger.info(f"Type: {stage_type}")
        logger.info(f"Description: {stage['description']}")
        logger.info(f"{'='*60}")
        
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
            logger.info(f"✅ Stage completed in {execution_time:.2f}ms")
            
        except Exception as e:
            logger.error(f"❌ Stage failed: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 1: Extract from Multiple SpaceX API Endpoints
    def _stage_extract(self, stage: dict):
        source = stage['source']
        base_url = source['base_url']
        
        logger.info(f"Fetching data from SpaceX API: {base_url}")
        
        try:
            # Fetch launches (past 100 launches)
            logger.info("Fetching launches...")
            launches_url = f"{base_url}/launches/past"
            response = requests.get(launches_url)
            response.raise_for_status()
            launches_data = response.json()
            
            # Take last 100 launches
            launches_data = launches_data[-100:]
            
            launches_list = []
            for launch in launches_data:
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
            
            self.launches_df = pd.DataFrame(launches_list)
            logger.info(f"✅ Loaded {len(self.launches_df)} launches")
            
            # Fetch rockets
            logger.info("Fetching rockets...")
            rockets_url = f"{base_url}/rockets"
            response = requests.get(rockets_url)
            response.raise_for_status()
            rockets_data = response.json()
            
            rockets_list = []
            for rocket in rockets_data:
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
                    'height_meters': rocket.get('height', {}).get('meters'),
                    'diameter_meters': rocket.get('diameter', {}).get('meters'),
                    'mass_kg': rocket.get('mass', {}).get('kg'),
                }
                rockets_list.append(rocket_obj)
            
            self.rockets_df = pd.DataFrame(rockets_list)
            logger.info(f"✅ Loaded {len(self.rockets_df)} rockets")
            
            # Fetch launchpads
            logger.info("Fetching launchpads...")
            launchpads_url = f"{base_url}/launchpads"
            response = requests.get(launchpads_url)
            response.raise_for_status()
            launchpads_data = response.json()
            
            launchpads_list = []
            for pad in launchpads_data:
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
            
            self.launchpads_df = pd.DataFrame(launchpads_list)
            logger.info(f"✅ Loaded {len(self.launchpads_df)} launchpads")
            
            logger.info(f"✅ Total data extracted - Launches: {len(self.launches_df)}, "
                       f"Rockets: {len(self.rockets_df)}, Launchpads: {len(self.launchpads_df)}")
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from SpaceX API: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 2: Enrich and Join Data
    def _stage_enrich(self, stage: dict):
        if self.launches_df is None or self.rockets_df is None or self.launchpads_df is None:
            raise ValueError("Source data not available.")
        
        logger.info("Enriching launch data with rocket and launchpad information...")
        
        try:
            # Join launches with rockets
            logger.info("Joining launches with rocket data...")
            enriched_df = self.launches_df.merge(
                self.rockets_df,
                on='rocket_id',
                how='left'
            )
            
            # Join with launchpads
            logger.info("Joining with launchpad data...")
            enriched_df = enriched_df.merge(
                self.launchpads_df,
                on='launchpad_id',
                how='left'
            )
            
            # Convert date fields
            logger.info("Converting date fields...")
            enriched_df['date_utc'] = pd.to_datetime(enriched_df['date_utc'])
            enriched_df['launch_year'] = enriched_df['date_utc'].dt.year
            enriched_df['launch_month'] = enriched_df['date_utc'].dt.month
            enriched_df['launch_day_of_week'] = enriched_df['date_utc'].dt.day_name()
            
            # Calculate launch cost efficiency
            enriched_df['cost_per_payload'] = (
                enriched_df['cost_per_launch'] / enriched_df['payloads']
            )

            # Handle invalid cases
            mask = (enriched_df['payloads'] <= 0) | (enriched_df['cost_per_launch'].isna())
            enriched_df.loc[mask, 'cost_per_payload'] = None
            
            self.launches_df = enriched_df
            
            logger.info(f"✅ Enriched {len(self.launches_df)} launch records")
            logger.info(f"Columns: {self.launches_df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to enrich data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 3: Data Quality Assessment and Branching
    def _stage_quality_branch(self, stage: dict):
        if self.launches_df is None:
            raise ValueError("No launch data to assess.")
        
        logger.info("Assessing data quality and branching...")
        
        try:
            # Define completeness criteria
            required_fields = ['success', 'rocket_name', 'launchpad_name', 
                             'cost_per_launch', 'details']
            
            # Calculate completeness score for each record
            self.launches_df['completeness_score'] = self.launches_df[required_fields].notna().sum(axis=1) / len(required_fields)
            
            # Branch based on completeness (>= 80% complete)
            complete_mask = self.launches_df['completeness_score'] >= 0.8
            
            self.complete_data_df = self.launches_df[complete_mask].copy()
            self.incomplete_data_df = self.launches_df[~complete_mask].copy()
            
            logger.info(f"✅ Data quality assessment complete:")
            logger.info(f"  → Complete data (≥80%): {len(self.complete_data_df)} launches")
            logger.info(f"  → Incomplete data (<80%): {len(self.incomplete_data_df)} launches")
            logger.info(f"  → Average completeness: {self.launches_df['completeness_score'].mean():.2%}")
            
        except Exception as e:
            logger.error(f"Failed to branch data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4a: Process Complete Data with Advanced Analytics
    def _stage_process_complete(self, stage: dict):
        if self.complete_data_df is None or len(self.complete_data_df) == 0:
            logger.warning("No complete data to process")
            self.complete_data_df = pd.DataFrame()
            return
        
        logger.info("Processing complete data with advanced analytics...")
        
        try:
            df = self.complete_data_df.copy()
            
            # Calculate success metrics
            df['mission_outcome'] = df['success'].apply(
                lambda x: 'Success' if x == True else 'Failure' if x == False else 'Unknown'
            )
            
            # Calculate rocket reliability score
            df['reliability_score'] = df.apply(
                lambda row: (row['success_rate'] / 100) * row['completeness_score']
                if pd.notna(row['success_rate']) else row['completeness_score'],
                axis=1
            )
            
            # Determine launch complexity
            df['mission_complexity'] = df.apply(
                lambda row: 'High' if row['crew'] > 0 or row['payloads'] > 3
                else 'Medium' if row['payloads'] > 1
                else 'Low',
                axis=1
            )
            
            # Calculate days since launch
            df['days_since_launch'] = df['date_utc'].apply(lambda x: (pd.Timestamp.now() - x).days if pd.notnull(x) else None)
            
            # Add data processing tier
            df['processing_tier'] = 'complete_analytics'
            
            self.complete_data_df = df
            
            logger.info(f"✅ Processed {len(self.complete_data_df)} complete records")
            logger.info(f"Mission outcomes: {df['mission_outcome'].value_counts().to_dict()}")
            logger.info(f"Complexity distribution: {df['mission_complexity'].value_counts().to_dict()}")
            
        except Exception as e:
            logger.error(f"Failed to process complete data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4b: Process Incomplete Data with Basic Metrics
    def _stage_process_incomplete(self, stage: dict):
        if self.incomplete_data_df is None or len(self.incomplete_data_df) == 0:
            logger.warning("No incomplete data to process")
            self.incomplete_data_df = pd.DataFrame()
            return
        
        logger.info("Processing incomplete data with basic metrics...")
        
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
                df['days_since_launch'] = (pd.Timestamp.now() - pd.to_datetime(df['date_utc'])).dt.days
            else:
                df['days_since_launch'] = None
            
            # Add data processing tier
            df['processing_tier'] = 'basic_metrics'
            
            self.incomplete_data_df = df
            
            logger.info(f"✅ Processed {len(self.incomplete_data_df)} incomplete records")
            logger.info(f"Mission outcomes: {df['mission_outcome'].value_counts().to_dict()}")
            
        except Exception as e:
            logger.error(f"Failed to process incomplete data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 5: Merge Branches, Add Final Enrichments, and Load
    def _stage_merge_and_load(self, stage: dict):
        logger.info("Merging processing branches...")
        
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
            logger.info("Adding final statistical enrichments...")
            
            # Calculate aggregate statistics
            self.final_df['avg_success_rate_by_rocket'] = self.final_df.groupby('rocket_name')['success'].transform('mean')
            self.final_df['launches_by_rocket'] = self.final_df.groupby('rocket_name')['rocket_name'].transform('count')
            self.final_df['avg_success_rate_by_pad'] = self.final_df.groupby('launchpad_name')['success'].transform('mean')
            
            # Add timestamp
            self.final_df['processed_at'] = pd.Timestamp.now()
            
            logger.info(f"✅ Merged {len(self.final_df)} total records")
            logger.info(f"Processing tier distribution:")
            logger.info(f"{self.final_df['processing_tier'].value_counts().to_dict()}")
            
            # Load to database
            destination = stage['destination']
            table_name = destination['table_name']
            
            logger.info(f"Loading {len(self.final_df)} rows to table: {table_name}")
            
            # Get database URL
            database_url = os.environ.get("AIVEN_PG_URI") or os.environ.get("DATABASE_URL")
            if not database_url:
                raise ValueError("No database connection string found.")
            
            # Fix dialect for SQLAlchemy
            if database_url.startswith("postgres://"):
                database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
            
            # Create engine
            engine = create_engine(database_url)
            logger.info("Database connection established")
            
            # Drop table if exists
            with engine.connect() as conn:
                logger.info(f"Dropping existing table if exists: {table_name}")
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                conn.commit()
            
            # Load data
            logger.info("Writing data to database...")
            self.final_df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"✅ {len(self.final_df)} rows inserted into {table_name}")
            
            # Create indexes
            if destination.get('create_indexes'):
                index_columns = destination.get('index_columns', [])
                logger.info(f"Creating indexes on: {index_columns}")
                with engine.connect() as conn:
                    for col in index_columns:
                        if col in self.final_df.columns:
                            index_name = f"idx_{table_name}_{col}"
                            try:
                                conn.execute(text(
                                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col})"
                                ))
                                logger.info(f"  ✅ Created index: {index_name}")
                            except Exception as e:
                                logger.warning(f"  ⚠️ Failed to create index on {col}: {e}")
                    conn.commit()
            
            # Verify row count
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                logger.info(f"✅ Verified: {count} rows in {table_name}")
            
            engine.dispose()
            
        except Exception as e:
            logger.error(f"Failed to merge and load data: {e}")
            raise

if __name__ == "__main__":
    exit(main())