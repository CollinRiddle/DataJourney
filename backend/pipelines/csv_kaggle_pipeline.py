# ------------------------------------------------------------------- #
# CSV to PostgreSQL ETL Pipeline
# Extract multiple CSV datasets from Kaggle, Transform, Load to PostgreSQL
#
# This pipeline demonstrates:
# 1. Loading multiple CSV datasets from Kaggle
# 2. Merging and transforming data from different sources
# 3. Creating fact and dimension tables in PostgreSQL
#
# Datasets:
# - Customer Shopping Data (retail transactions)
# - Video Game Sales (gaming industry analytics)
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
        pipeline = CSVKagglePipeline()
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
# CSV to PostgreSQL Pipeline Class

class CSVKagglePipeline:

    # ---------------------------------------------------------- #
    # * Initialize the pipeline with given configuration *
    # 
    # 1. Load config from JSON
    # 2. Validate environment variables
    # 3. Confirm pipeline from config file

    def __init__(self, config_path: str = "backend/data_config/pipeline_config.json"):

        logger.info("Initializing CSV Kaggle Pipeline...")
        
        # Load environment variables from .env file
        load_dotenv()
        
        # Load configuration from JSON
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # Get the csv_kaggle pipeline config
            self.pipeline_config = None
            for pipeline in self.config['pipelines']:
                if pipeline['pipeline_id'] == 'csv_kaggle':
                    self.pipeline_config = pipeline
                    break
            
            if not self.pipeline_config:
                raise ValueError("Pipeline 'csv_kaggle' not found in config")
            
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
        
        self.shopping_df: Optional[pd.DataFrame] = None
        self.games_df: Optional[pd.DataFrame] = None
        self.merged_df: Optional[pd.DataFrame] = None
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
            if stage['stage_id'] == 'extract_shopping_data':
                self._stage_extract_shopping(stage)
            elif stage['stage_id'] == 'extract_games_data':
                self._stage_extract_games(stage)
            elif stage['stage_id'] == 'transform_shopping':
                self._stage_transform_shopping(stage)
            elif stage['stage_id'] == 'transform_games':
                self._stage_transform_games(stage)
            elif stage['stage_id'] == 'merge_datasets':
                self._stage_merge_datasets(stage)
            elif stage['stage_id'] == 'load_shopping_to_postgres':
                self._stage_load_shopping(stage)
            elif stage['stage_id'] == 'load_games_to_postgres':
                self._stage_load_games(stage)
            elif stage['stage_id'] == 'load_merged_to_postgres':
                self._stage_load_merged(stage)
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
    # Stage 1: Extract Shopping Data from Kaggle

    def _stage_extract_shopping(self, stage: dict):

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
            self.shopping_df = pd.read_csv(csv_path)
            
            # Limit to 200 rows for consistency
            self.shopping_df = self.shopping_df.head(200)
            
            logger.info(f"Loaded {len(self.shopping_df)} rows, {len(self.shopping_df.columns)} columns")
            logger.info(f"Columns: {self.shopping_df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to extract shopping data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 2: Extract Video Game Sales Data from Kaggle

    def _stage_extract_games(self, stage: dict):

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
            self.games_df = pd.read_csv(csv_path)
            
            # Limit to 200 rows for consistency
            self.games_df = self.games_df.head(200)
            
            logger.info(f"Loaded {len(self.games_df)} rows, {len(self.games_df.columns)} columns")
            logger.info(f"Columns: {self.games_df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to extract games data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 3: Transform Shopping Data

    def _stage_transform_shopping(self, stage: dict):

        if self.shopping_df is None:
            raise ValueError("No shopping data to transform. Extract stage must run first.")
        
        initial_rows = len(self.shopping_df)
        logger.info(f"Starting transformation with {initial_rows} rows")
        
        try:
            # Rename columns to snake_case
            logger.info("Transformation 1: Renaming columns to snake_case")
            self.shopping_df.columns = [col.lower().replace(' ', '_') for col in self.shopping_df.columns]
            logger.info(f"Columns renamed: {self.shopping_df.columns.tolist()}")
            
            # Convert date column if exists
            if 'invoice_date' in self.shopping_df.columns:
                logger.info("Transformation 2: Converting date column to datetime")
                self.shopping_df['invoice_date'] = pd.to_datetime(self.shopping_df['invoice_date'], errors='coerce')
                self.shopping_df['year'] = self.shopping_df['invoice_date'].dt.year
                self.shopping_df['month'] = self.shopping_df['invoice_date'].dt.month
                self.shopping_df['day_of_week'] = self.shopping_df['invoice_date'].dt.day_name()
            
            # Calculate total amount if price and quantity exist
            if 'price' in self.shopping_df.columns and 'quantity' in self.shopping_df.columns:
                logger.info("Transformation 3: Calculating total amount")
                self.shopping_df['total_amount'] = self.shopping_df['price'] * self.shopping_df['quantity']
            
            # Categorize customers by age
            if 'age' in self.shopping_df.columns:
                logger.info("Transformation 4: Categorizing customers by age group")
                self.shopping_df['age_group'] = pd.cut(
                    self.shopping_df['age'],
                    bins=[0, 18, 30, 45, 60, 100],
                    labels=['Under 18', '18-30', '31-45', '46-60', 'Over 60']
                )
            
            # Add spending tier based on total amount
            if 'total_amount' in self.shopping_df.columns:
                logger.info("Transformation 5: Creating spending tier categories")
                self.shopping_df['spending_tier'] = pd.cut(
                    self.shopping_df['total_amount'],
                    bins=[0, 50, 200, 500, float('inf')],
                    labels=['Low', 'Medium', 'High', 'Premium']
                )
            
            logger.info(f"Transformation complete: {len(self.shopping_df)} rows remaining")
            logger.info(f"Final columns: {self.shopping_df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to transform shopping data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4: Transform Video Game Sales Data

    def _stage_transform_games(self, stage: dict):

        if self.games_df is None:
            raise ValueError("No games data to transform. Extract stage must run first.")
        
        initial_rows = len(self.games_df)
        logger.info(f"Starting transformation with {initial_rows} rows")
        
        try:
            # Rename columns to snake_case
            logger.info("Transformation 1: Renaming columns to snake_case")
            self.games_df.columns = [col.lower().replace(' ', '_') for col in self.games_df.columns]
            logger.info(f"Columns renamed: {self.games_df.columns.tolist()}")
            
            # Calculate total global sales if regional sales exist
            sales_columns = [col for col in self.games_df.columns if 'sales' in col.lower()]
            
            # Convert all sales columns to numeric first
            if len(sales_columns) > 0:
                logger.info("Transformation 2: Converting sales columns to numeric")
                for col in sales_columns:
                    self.games_df[col] = pd.to_numeric(self.games_df[col], errors='coerce')
            
            if len(sales_columns) > 1:
                logger.info("Transformation 3: Calculating total global sales")
                # Create global_sales if it doesn't exist
                if 'global_sales' not in self.games_df.columns:
                    self.games_df['global_sales'] = self.games_df[sales_columns].sum(axis=1)
            
            # Categorize by sales performance
            if 'global_sales' in self.games_df.columns:
                logger.info("Transformation 4: Categorizing by sales performance")
                self.games_df['sales_category'] = pd.cut(
                    self.games_df['global_sales'],
                    bins=[0, 0.5, 2, 5, float('inf')],
                    labels=['Niche', 'Moderate', 'Popular', 'Blockbuster']
                )
            
            # Add decade classification if year exists
            if 'year' in self.games_df.columns:
                logger.info("Transformation 5: Adding decade classification")
                self.games_df['year'] = pd.to_numeric(self.games_df['year'], errors='coerce')
                self.games_df['decade'] = (self.games_df['year'] // 10 * 10).astype('Int64')
            
            # Determine primary market (highest regional sales)
            regional_sales_cols = [col for col in self.games_df.columns if 'sales' in col.lower() and col != 'global_sales']
            if len(regional_sales_cols) > 0:
                logger.info("Transformation 6: Identifying primary market region")
                # Create a copy with numeric values and fill NaN with 0
                regional_sales_df = self.games_df[regional_sales_cols].copy()
                for col in regional_sales_cols:
                    regional_sales_df[col] = pd.to_numeric(regional_sales_df[col], errors='coerce').fillna(0)
                
                self.games_df['primary_market'] = regional_sales_df.idxmax(axis=1)
                self.games_df['primary_market'] = self.games_df['primary_market'].str.replace('_sales', '')
            
            logger.info(f"Transformation complete: {len(self.games_df)} rows remaining")
            logger.info(f"Final columns: {self.games_df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to transform games data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 5: Merge Datasets for Cross-Analysis

    def _stage_merge_datasets(self, stage: dict):

        if self.shopping_df is None or self.games_df is None:
            raise ValueError("Both datasets must be loaded before merging.")
        
        logger.info("Creating merged analysis dataset")
        
        try:
            # Create age-based aggregation from shopping data
            if 'age_group' in self.shopping_df.columns and 'total_amount' in self.shopping_df.columns:
                shopping_summary = self.shopping_df.groupby('age_group').agg({
                    'total_amount': ['sum', 'mean', 'count']
                }).reset_index()
                shopping_summary.columns = ['age_group', 'total_revenue', 'avg_transaction', 'transaction_count']
            
            # Create decade-based aggregation from games data
            if 'decade' in self.games_df.columns and 'global_sales' in self.games_df.columns:
                games_summary = self.games_df.groupby('decade').agg({
                    'global_sales': ['sum', 'mean', 'count']
                }).reset_index()
                games_summary.columns = ['decade', 'total_sales_millions', 'avg_sales', 'game_count']
            
            # Create a cross-reference analysis
            # Map age groups to gaming decades (people in each age group likely grew up in certain decades)
            age_decade_mapping = []
            
            if 'shopping_summary' in locals() and 'games_summary' in locals():
                for _, shop_row in shopping_summary.iterrows():
                    for _, game_row in games_summary.iterrows():
                        age_decade_mapping.append({
                            'age_group': shop_row['age_group'],
                            'decade': game_row['decade'],
                            'shopping_revenue': shop_row['total_revenue'],
                            'shopping_transactions': shop_row['transaction_count'],
                            'avg_shopping_amount': shop_row['avg_transaction'],
                            'gaming_sales_millions': game_row['total_sales_millions'],
                            'games_released': game_row['game_count'],
                            'avg_game_sales': game_row['avg_sales']
                        })
                
                self.merged_df = pd.DataFrame(age_decade_mapping)
                logger.info(f"Created merged dataset with {len(self.merged_df)} rows")
                logger.info(f"Merged columns: {self.merged_df.columns.tolist()}")
            else:
                logger.warning("Could not create merged dataset - missing required columns")
                self.merged_df = pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Failed to merge datasets: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 6: Load Shopping Data to PostgreSQL

    def _stage_load_shopping(self, stage: dict):

        if self.shopping_df is None:
            raise ValueError("No shopping data to load. Previous stages must complete first.")

        destination = stage['destination']
        table_name = destination['table_name']

        logger.info(f"Loading {len(self.shopping_df)} rows to table: {table_name}")

        try:
            # Get database URL from environment
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
            self.shopping_df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"✅ {len(self.shopping_df)} rows inserted into {table_name}")

            # Create indexes if specified
            if destination.get('create_indexes'):
                index_columns = destination.get('index_columns', [])
                logger.info(f"Creating indexes on: {index_columns}")
                with engine.connect() as conn:
                    for col in index_columns:
                        if col in self.shopping_df.columns:
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
            logger.error(f"Failed to load shopping data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 7: Load Video Game Sales Data to PostgreSQL

    def _stage_load_games(self, stage: dict):

        if self.games_df is None:
            raise ValueError("No games data to load. Previous stages must complete first.")

        destination = stage['destination']
        table_name = destination['table_name']

        logger.info(f"Loading {len(self.games_df)} rows to table: {table_name}")

        try:
            # Get database URL from environment
            database_url = os.environ.get("AIVEN_PG_URI") or os.environ.get("DATABASE_URL")
            if not database_url:
                raise ValueError(
                    "No database connection string found. "
                    "Set AIVEN_PG_URI or DATABASE_URL in your environment."
                )

            # Fix dialect for SQLAlchemy
            if database_url.startswith("postgres://"):
                database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)

            # Create engine
            import psycopg2
            engine = create_engine(database_url)
            logger.info("Database connection established")

            # Drop table if exists
            with engine.connect() as conn:
                logger.info(f"Dropping existing table if exists: {table_name}")
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                conn.commit()

            # Load data
            logger.info("Writing data to database...")
            self.games_df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"✅ {len(self.games_df)} rows inserted into {table_name}")

            # Create indexes if specified
            if destination.get('create_indexes'):
                index_columns = destination.get('index_columns', [])
                logger.info(f"Creating indexes on: {index_columns}")
                with engine.connect() as conn:
                    for col in index_columns:
                        if col in self.games_df.columns:
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

        except Exception as e:
            logger.error(f"Failed to load games data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 8: Load Merged Analysis Data to PostgreSQL

    def _stage_load_merged(self, stage: dict):

        if self.merged_df is None or len(self.merged_df) == 0:
            logger.warning("No merged data to load. Skipping this stage.")
            return

        destination = stage['destination']
        table_name = destination['table_name']

        logger.info(f"Loading {len(self.merged_df)} rows to table: {table_name}")

        try:
            # Get database URL from environment
            database_url = os.environ.get("AIVEN_PG_URI") or os.environ.get("DATABASE_URL")
            if not database_url:
                raise ValueError(
                    "No database connection string found. "
                    "Set AIVEN_PG_URI or DATABASE_URL in your environment."
                )

            # Fix dialect for SQLAlchemy
            if database_url.startswith("postgres://"):
                database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)

            # Create engine
            import psycopg2
            engine = create_engine(database_url)
            logger.info("Database connection established")

            # Drop table if exists
            with engine.connect() as conn:
                logger.info(f"Dropping existing table if exists: {table_name}")
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                conn.commit()

            # Load data
            logger.info("Writing data to database...")
            self.merged_df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"✅ {len(self.merged_df)} rows inserted into {table_name}")

            # Create indexes if specified
            if destination.get('create_indexes'):
                index_columns = destination.get('index_columns', [])
                logger.info(f"Creating indexes on: {index_columns}")
                with engine.connect() as conn:
                    for col in index_columns:
                        if col in self.merged_df.columns:
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

        except Exception as e:
            logger.error(f"Failed to load merged data: {e}")
            raise

if __name__ == "__main__":
    main()
