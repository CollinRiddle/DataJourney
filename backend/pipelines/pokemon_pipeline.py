# ------------------------------------------------------------------- #
# Pokémon Data Pipeline
# Extract from PokeAPI, Transform with Branching, Load to PostgreSQL
#
# DATA LIMIT: 50 Pokemon (to conserve space and respect API)
#
# Pipeline Flow:
# 1. Extract from PokeAPI
# 2. Clean and Transform
# 3. Branch: Legendary vs Non-Legendary Processing
# 4. Merge branches and Load to PostgreSQL
# ------------------------------------------------------------------- #

import os
import json
import logging
import time
import requests
from typing import Optional, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------------------------- #
# Main function to run the pipeline

def main():
    try:
        pipeline = PokemonPipeline()
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
# Pokémon Pipeline Class

class PokemonPipeline:

    def __init__(self, config_path: str = "backend/data_config/pipeline_config.json"):
        logger.info("Initializing Pokémon Pipeline...")
        
        # Load environment variables
        load_dotenv()
        
        # Load configuration
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
            
            # Get the pokemon_data pipeline config
            self.pipeline_config = None
            for pipeline in self.config['pipelines']:
                if pipeline['pipeline_id'] == 'pokemon_data':
                    self.pipeline_config = pipeline
                    break
            
            if not self.pipeline_config:
                raise ValueError("Pipeline 'pokemon_data' not found in config")
            
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
        self.legendary_df: Optional[pd.DataFrame] = None
        self.non_legendary_df: Optional[pd.DataFrame] = None
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
            logger.info(f"Final record count: {len(self.df) if self.df is not None else 0}")
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
            if stage['stage_id'] == 'extract_pokeapi':
                self._stage_extract(stage)
            elif stage['stage_id'] == 'transform_pokemon':
                self._stage_transform(stage)
            elif stage['stage_id'] == 'branch_legendary':
                self._stage_branch(stage)
            elif stage['stage_id'] == 'process_legendary':
                self._stage_process_legendary(stage)
            elif stage['stage_id'] == 'process_non_legendary':
                self._stage_process_non_legendary(stage)
            elif stage['stage_id'] == 'merge_and_load':
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
    # Stage 1: Extract from PokeAPI
    def _stage_extract(self, stage: dict):
        source = stage['source']
        base_url = source['base_url']
        # LIMIT TO 50 POKEMON TO CONSERVE SPACE
        limit = 50
        
        logger.info(f"Fetching {limit} Pokémon from PokeAPI (limited for space conservation)...")
        
        try:
            pokemon_list = []
            
            # Fetch basic info for each Pokémon
            for i in range(1, limit + 1):
                try:
                    url = f"{base_url}/pokemon/{i}"
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    
                    pokemon_data = response.json()
                    
                    # Fetch species data for legendary status
                    species_url = pokemon_data['species']['url']
                    species_response = requests.get(species_url, timeout=10)
                    species_response.raise_for_status()
                    species_data = species_response.json()
                    
                    # Extract relevant fields with safe defaults
                    pokemon = {
                        'pokemon_id': pokemon_data.get('id'),
                        'name': pokemon_data.get('name', 'unknown'),
                        'height': pokemon_data.get('height', 0),
                        'weight': pokemon_data.get('weight', 0),
                        'base_experience': pokemon_data.get('base_experience', 0),
                        'hp': pokemon_data['stats'][0]['base_stat'] if len(pokemon_data.get('stats', [])) > 0 else 0,
                        'attack': pokemon_data['stats'][1]['base_stat'] if len(pokemon_data.get('stats', [])) > 1 else 0,
                        'defense': pokemon_data['stats'][2]['base_stat'] if len(pokemon_data.get('stats', [])) > 2 else 0,
                        'special_attack': pokemon_data['stats'][3]['base_stat'] if len(pokemon_data.get('stats', [])) > 3 else 0,
                        'special_defense': pokemon_data['stats'][4]['base_stat'] if len(pokemon_data.get('stats', [])) > 4 else 0,
                        'speed': pokemon_data['stats'][5]['base_stat'] if len(pokemon_data.get('stats', [])) > 5 else 0,
                        'type_primary': pokemon_data['types'][0]['type']['name'] if len(pokemon_data.get('types', [])) > 0 else 'normal',
                        'type_secondary': pokemon_data['types'][1]['type']['name'] if len(pokemon_data.get('types', [])) > 1 else None,
                        'is_legendary': species_data.get('is_legendary', False),
                        'is_mythical': species_data.get('is_mythical', False),
                        'generation': species_data.get('generation', {}).get('name', 'unknown'),
                    }
                    
                    pokemon_list.append(pokemon)
                    
                    if i % 10 == 0:
                        logger.info(f"  Fetched {i}/{limit} Pokémon...")
                    
                    # Be respectful to the API - add small delay
                    time.sleep(0.1)
                    
                except requests.RequestException as e:
                    logger.warning(f"Failed to fetch Pokemon {i}: {e}")
                    continue
                except (KeyError, IndexError) as e:
                    logger.warning(f"Data parsing error for Pokemon {i}: {e}")
                    continue
            
            # Create DataFrame
            self.df = pd.DataFrame(pokemon_list)
            
            logger.info(f"✅ Loaded {len(self.df)} Pokémon with {len(self.df.columns)} attributes")
            logger.info(f"Columns: {self.df.columns.tolist()}")
            
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            raise

    # ---------------------------------------------------------- #
    # Stage 2: Transform and Clean
    def _stage_transform(self, stage: dict):
        if self.df is None:
            raise ValueError("No data to transform.")
        
        initial_rows = len(self.df)
        logger.info(f"Starting transformation with {initial_rows} rows")
        
        try:
            # Calculate total stats
            logger.info("Calculating total stats...")
            self.df['total_stats'] = (
                self.df['hp'] + self.df['attack'] + self.df['defense'] +
                self.df['special_attack'] + self.df['special_defense'] + self.df['speed']
            )
            
            # Determine rarity tier
            logger.info("Calculating rarity tiers...")
            self.df['rarity'] = self.df.apply(lambda row: 
                'mythical' if row['is_mythical']
                else 'legendary' if row['is_legendary']
                else 'rare' if row['total_stats'] >= 500
                else 'uncommon' if row['total_stats'] >= 400
                else 'common',
                axis=1
            )
            
            # Capitalize names
            logger.info("Formatting names...")
            self.df['name'] = self.df['name'].str.title()
            
            # Add processing timestamp
            self.df['processed_at'] = pd.Timestamp.now()
            
            logger.info(f"✅ Transformation complete: {len(self.df)} rows")
            logger.info(f"Rarity distribution:\n{self.df['rarity'].value_counts()}")
            
        except Exception as e:
            logger.error(f"Failed to transform data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 3: Branch Decision Point
    def _stage_branch(self, stage: dict):
        if self.df is None:
            raise ValueError("No data to branch.")
        
        logger.info("Branching data based on legendary status...")
        
        try:
            # Split into legendary and non-legendary
            legendary_mask = (self.df['is_legendary'] == True) | (self.df['is_mythical'] == True)
            
            self.legendary_df = self.df[legendary_mask].copy()
            self.non_legendary_df = self.df[~legendary_mask].copy()
            
            logger.info(f"✅ Branch complete:")
            logger.info(f"  → Legendary/Mythical: {len(self.legendary_df)} Pokémon")
            logger.info(f"  → Non-Legendary: {len(self.non_legendary_df)} Pokémon")
            
        except Exception as e:
            logger.error(f"Failed to branch data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4a: Process Legendary Pokémon
    def _stage_process_legendary(self, stage: dict):
        if self.legendary_df is None:
            raise ValueError("No legendary data to process.")
        
        logger.info("Processing legendary Pokémon...")
        
        try:
            # Handle empty legendary dataframe
            if len(self.legendary_df) == 0:
                logger.warning("No legendary Pokemon in this dataset")
                self.legendary_df['legendary_tier'] = pd.Series(dtype='object')
                self.legendary_df['power_score'] = pd.Series(dtype='float64')
                return
            
            # Add special legendary tier
            self.legendary_df['legendary_tier'] = self.legendary_df.apply(lambda row:
                'mythical' if row['is_mythical']
                else 'box_legendary' if row['total_stats'] >= 680
                else 'sub_legendary',
                axis=1
            )
            
            # Calculate legendary power score
            self.legendary_df['power_score'] = (
                self.legendary_df['total_stats'] * 1.5 +
                self.legendary_df['base_experience']
            )
            
            logger.info(f"✅ Processed {len(self.legendary_df)} legendary Pokémon")
            if len(self.legendary_df) > 0:
                logger.info(f"Legendary tiers:\n{self.legendary_df['legendary_tier'].value_counts()}")
            
        except Exception as e:
            logger.error(f"Failed to process legendary data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 4b: Process Non-Legendary Pokémon
    def _stage_process_non_legendary(self, stage: dict):
        if self.non_legendary_df is None:
            raise ValueError("No non-legendary data to process.")
        
        logger.info("Processing non-legendary Pokémon...")
        
        try:
            # Add placeholder for legendary tier
            self.non_legendary_df['legendary_tier'] = None
            
            # Calculate standard power score
            self.non_legendary_df['power_score'] = (
                self.non_legendary_df['total_stats'] +
                self.non_legendary_df['base_experience'] * 0.5
            )
            
            # Determine combat role
            self.non_legendary_df['combat_role'] = self.non_legendary_df.apply(lambda row:
                'sweeper' if row['attack'] > row['defense'] and row['speed'] >= 80
                else 'tank' if row['defense'] > row['attack'] and row['hp'] >= 80
                else 'balanced',
                axis=1
            )
            
            logger.info(f"✅ Processed {len(self.non_legendary_df)} non-legendary Pokémon")
            logger.info(f"Combat roles:\n{self.non_legendary_df['combat_role'].value_counts()}")
            
        except Exception as e:
            logger.error(f"Failed to process non-legendary data: {e}")
            raise
    
    # ---------------------------------------------------------- #
    # Stage 5: Merge and Load to PostgreSQL
    def _stage_merge_and_load(self, stage: dict):
        if self.legendary_df is None or self.non_legendary_df is None:
            raise ValueError("Branch data not available for merging.")
        
        logger.info("Merging branches...")
        
        try:
            # Ensure both DataFrames have the same columns
            # Add missing columns with default values
            if 'combat_role' not in self.legendary_df.columns:
                self.legendary_df['combat_role'] = None
            
            # Merge the branches
            self.df = pd.concat([self.legendary_df, self.non_legendary_df], ignore_index=True)
            
            logger.info(f"✅ Merged {len(self.df)} total Pokémon")
            
            # Load to database
            destination = stage['destination']
            table_name = destination['table_name']
            
            logger.info(f"Loading {len(self.df)} rows to table: {table_name}")
            
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
            self.df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"✅ {len(self.df)} rows inserted into {table_name}")
            
            # Create indexes
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
                logger.info(f"✅ Verified: {count} rows in {table_name}")
            
            engine.dispose()
            
        except Exception as e:
            logger.error(f"Failed to merge and load data: {e}")
            raise

if __name__ == "__main__":
    exit(main())