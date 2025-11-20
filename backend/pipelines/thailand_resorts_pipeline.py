# ------------------------------------------------------------------- #
# Thailand Resorts ETL Pipeline
# Extract from Kaggle, Transform, Load to PostgreSQL
#
# Transformations:
# 1. Rename columns to snake_case
# 2. Parse price to numeric
# 3. Extract review counts
# ------------------------------------------------------------------- #

import os
import json
import logging
import time
from typing import Optional

import pandas as pd
import kagglehub
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------------------------- #
# Main function to run the pipeline

def main():

    try:
        pipeline = HotelPipeline()
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
# Thailand Hotels Pipeline Class

class HotelPipeline:

    # ---------------------------------------------------------- #
    # * Initialize the pipeline with given configuration *
    # 
    # 1. Load config from JSON
    # 2. Validate environment variables
    # 3. Confirm pipeline from config file

    def __init__(self, config_path: str = "backend/data_config/pipeline_config.json"):

        logger.info("Initializing Hotel Pipeline...")
        
        # Load environment variables from .env file
        load_dotenv()
        
        # Load configuration from JSON
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # Get the thailand_hotels pipeline config
            self.pipeline_config = None
            for pipeline in self.config['pipelines']:
                if pipeline['pipeline_id'] == 'thailand_hotels':
                    self.pipeline_config = pipeline
                    break
            
            if not self.pipeline_config:
                raise ValueError("Pipeline 'thailand_hotels' not found in config")
            
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
        
        self.df: Optional[pd.DataFrame] = None
        self.stage_timings = {}
    
    # ---------------------------------------------------------- #
    # Executes the full pipeline based on the configuration
    def run(self):
        """Execute the complete pipeline."""
        logger.info("="*60)
        if not self.pipeline_config:
            raise ValueError("Pipeline configuration not loaded. Please check your config file and initialization.")
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
            logger.info(f"Final record count: {len(self.df) if self.df is not None else 0}")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
            return False
    
    # ---------------------------------------------------------- #
    # Executes a single stage based on its type 
    # Primarily used for obtaining more detailed logging and timing

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
            if stage['stage_id'] == 'extract_kaggle_data':
                self._stage_extract(stage)
            elif stage['stage_id'] == 'transform_hotel_data':
                self._stage_transform(stage)
            elif stage['stage_id'] == 'load_to_postgres':
                self._stage_load(stage)
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
    # Stage 1: Extract data from Kaggle
    # Download dataset from Kaggle and load into DataFrame

    def _stage_extract(self, stage: dict):

        # Extract the Kaggle dataset ID from config
        source = stage['source']
        dataset_id = source['dataset_id']
        
        logger.info(f"Downloading dataset: {dataset_id}")
        
        try:
            # Download dataset from Kaggle
            path = kagglehub.dataset_download(dataset_id)
            logger.info(f"Dataset downloaded to: {path}")
            
            # Find CSV file
            files = os.listdir(path)
            csv_files = [f for f in files if f.endswith('.csv')]
            
            if not csv_files:
                raise FileNotFoundError("No CSV files found in downloaded dataset")
            
            csv_file = csv_files[0]
            csv_path = os.path.join(path, csv_file)
            logger.info(f"Reading CSV file: {csv_file}")
            
            # Load into DataFrame
            self.df = pd.read_csv(csv_path)
            
            # Limit to 200 rows
            self.df = self.df.head(200)
            
            logger.info(f"Loaded {len(self.df)} rows, {len(self.df.columns)} columns")
            logger.info(f"Columns: {self.df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 2: Transform data
    # Apply transformations and standardizations per the configuration file

    def _stage_transform(self, stage: dict):

        if self.df is None:
            raise ValueError("No data to transform. Extract stage must run first.")
        
        initial_rows = len(self.df)
        logger.info(f"Starting transformation with {initial_rows} rows")
        
        try:
            # Transformation 1: Rename columns 
            rename_mapping = stage['transformations'][0]['mapping']
            logger.info("Transformation 1: Renaming columns to snake_case")
            self.df = self.df.rename(columns=rename_mapping)
            logger.info(f"Columns renamed: {list(rename_mapping.values())}")
            
            # Transformation 2: Parse price
            logger.info("Transformation 2: Parsing price values")
            if 'price' in self.df.columns:
                # Extract numeric value from 'US$32' format
                self.df['price_usd'] = (
                    self.df['price']
                    .astype(str)
                    .str.replace('US$', '', regex=False)
                    .str.replace(',', '', regex=False)
                    .str.strip()
                )
                # Convert to float, coerce errors to NaN
                self.df['price_usd'] = pd.to_numeric(self.df['price_usd'], errors='coerce')
                
                non_null_prices = self.df['price_usd'].notna().sum()
                logger.info(f"Parsed {non_null_prices} valid prices")
            
            # Transformation 3: Extract review count (from config)
            logger.info("Transformation 3: Extracting review counts")
            if 'total_reviews' in self.df.columns:
                # Extract number from '100 reviews' format
                self.df['review_count'] = (
                    self.df['total_reviews']
                    .astype(str)
                    .str.extract(r'(\d+)', expand=False)
                    .astype(float)
                )
                
                non_null_reviews = self.df['review_count'].notna().sum()
                logger.info(f"Extracted {non_null_reviews} review counts")
            
            logger.info(f"Transformation complete: {len(self.df)} rows remaining")
            logger.info(f"Final columns: {self.df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to transform data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 2: Load data to PostgreSQL
    # Load the transformed DataFrame into the specified PostgreSQL table

    def _stage_load(self, stage: dict):

        if self.df is None:
            raise ValueError("No data to load. Previous stages must complete first.")

        destination = stage['destination']
        table_name = destination['table_name']

        logger.info(f"Loading {len(self.df)} rows to table: {table_name}")

        try:
            # Get database URL from Aiven or environment
            database_url = os.environ.get("AIVEN_PG_URI") or os.environ.get("DATABASE_URL")
            if not database_url:
                raise ValueError(
                    "No database connection string found. "
                    "Set AIVEN_PG_URI or DATABASE_URL in your environment."
                )

            # Fix dialect for SQLAlchemy
            if database_url.startswith("postgres://"):
                logger.info("Rewriting 'postgres://' to 'postgresql+psycopg2://' for SQLAlchemy")
                database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)

            # Create engine
            import psycopg2  # Ensure driver is installed
            engine = create_engine(database_url)
            logger.info("Database connection established")

            # Drop table if exists (clean slate)
            with engine.connect() as conn:
                logger.info(f"Dropping existing table if exists: {table_name}")
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                conn.commit()

            # Load data
            logger.info("Writing data to database...")
            self.df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"✅ {len(self.df)} rows inserted into {table_name}")

            # Create indexes if specified
            if destination.get('create_indexes'):
                index_columns = destination.get('index_columns', [])
                logger.info(f"Creating indexes on: {index_columns}")
                with engine.connect() as conn:
                    for col in index_columns:
                        if col in self.df.columns:
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
                logger.info(f"Verified: {count} rows in {table_name}")

            engine.dispose()

        except ImportError:
            logger.error("❌ psycopg2 driver not installed. Run 'pip install psycopg2-binary'")
            raise
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise

if __name__ == "__main__":
    main()