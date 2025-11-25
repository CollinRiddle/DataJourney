# ------------------------------------------------------------------- #
# Hacker News Scraper Pipeline
# Beginner Web Scraping ETL
#
# DATA LIMIT: 200 posts (multiple pages)
#
# Pipeline Flow:
# 1. Extract from Hacker News via Web Scraping (BeautifulSoup)
# 2. Transform & Clean Data
# 3. Load to PostgreSQL
# ------------------------------------------------------------------- #

import os
import logging
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------------------------- #
# Main function to run the pipeline

def main():
    try:
        pipeline = HackerNewsPipeline()
        success = pipeline.run()
        
        if success:
            print("\n🎉 Pipeline execution completed successfully!")
            return 0
        else:
            print("\n❌ Pipeline execution failed!")
            return 1
            
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        return 1

# -------------------------------------------------------------------------- #

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv("config/config.env")

class HackerNewsPipeline:
    """
    Web scraping pipeline that extracts front page posts from Hacker News,
    transforms the data, and loads to PostgreSQL.
    """
    
    def __init__(self):
        """Initialize pipeline with configuration"""
        logger.info("🔄 Initializing Hacker News Scraper Pipeline")
        
        self.base_url = "https://news.ycombinator.com"
        self.posts = []
        self.df = None
        
        # Load pipeline configuration
        import json
        config_path = os.path.join(os.path.dirname(__file__), '..', 'data_config', 'pipeline_config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Find this pipeline's config
        self.config = None
        for pipeline in config['pipelines']:
            if pipeline['pipeline_id'] == 'hackernews_scraper':
                self.config = pipeline
                break
        
        if not self.config:
            raise ValueError("Pipeline configuration not found for 'hackernews_scraper'")
        
        logger.info("✅ Pipeline initialized")
    
    def run(self):
        """Execute the complete pipeline"""
        try:
            logger.info("\n" + "="*60)
            logger.info("Starting Hacker News Scraper Pipeline")
            logger.info("="*60)
            
            # Execute each stage
            for stage in self.config['stages']:
                stage_id = stage['stage_id']
                
                if stage_id == 'scrape_hackernews':
                    self._stage_scrape()
                elif stage_id == 'transform_posts':
                    self._stage_transform()
                elif stage_id == 'load_posts':
                    self._stage_load(stage)
                else:
                    logger.warning(f"⚠️ Unknown stage: {stage_id}")
            
            logger.info("\n" + "="*60)
            logger.info("✅ Pipeline completed successfully")
            logger.info("="*60)
            return True
            
        except Exception as e:
            logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
            return False
    
    def _stage_scrape(self):
        """Stage 1: Scrape Hacker News front page posts"""
        logger.info("\n📥 Stage 1: Scraping Hacker News")
        
        try:
            pages_to_scrape = 7  # Each page has ~30 posts, so 7 pages ≈ 200 posts
            
            for page in range(1, pages_to_scrape + 1):
                url = f"{self.base_url}/?p={page}"
                logger.info(f"  Fetching page {page}...")
                
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all story rows
                story_rows = soup.select('tr.athing')
                
                for story in story_rows:
                    try:
                        # Extract story data
                        story_id = story.get('id')
                        title_element = story.select_one('.titleline > a')
                        
                        if not title_element:
                            continue
                        
                        title = title_element.text.strip()
                        url_link = title_element.get('href', '')
                        
                        # Get the subtext row (points, author, comments)
                        subtext = story.find_next_sibling('tr')
                        if not subtext:
                            continue
                        
                        subtext_cells = subtext.select_one('td.subtext')
                        if not subtext_cells:
                            continue
                        
                        # Extract points
                        score_elem = subtext_cells.select_one('.score')
                        points = 0
                        if score_elem:
                            points_text = score_elem.text.strip()
                            points = int(points_text.split()[0]) if points_text else 0
                        
                        # Extract author
                        author_elem = subtext_cells.select_one('.hnuser')
                        author = author_elem.text.strip() if author_elem else 'unknown'
                        
                        # Extract age
                        age_elem = subtext_cells.select_one('.age')
                        age = age_elem.text.strip() if age_elem else 'unknown'
                        
                        # Extract comment count
                        comments_elem = subtext_cells.find_all('a')[-1]
                        comments_text = comments_elem.text.strip()
                        comments = 0
                        if 'comment' in comments_text:
                            comments = int(comments_text.split()[0]) if comments_text.split()[0].isdigit() else 0
                        
                        # Determine source domain
                        source = 'news.ycombinator.com'
                        if url_link.startswith('http'):
                            from urllib.parse import urlparse
                            source = urlparse(url_link).netloc
                        
                        post = {
                            'story_id': story_id,
                            'title': title,
                            'url': url_link,
                            'points': points,
                            'author': author,
                            'age': age,
                            'comments': comments,
                            'source': source,
                            'scraped_at': datetime.utcnow()
                        }
                        
                        self.posts.append(post)
                        
                    except Exception as e:
                        logger.warning(f"  ⚠️ Error parsing story: {e}")
                        continue
                
                # Be polite - small delay between requests
                time.sleep(0.5)
            
            logger.info(f"✅ Scraped {len(self.posts)} posts from Hacker News")
            
        except Exception as e:
            logger.error(f"❌ Scraping failed: {e}")
            raise
    
    def _stage_transform(self):
        """Stage 2: Transform and clean scraped data"""
        logger.info("\n🔄 Stage 2: Transforming data")
        
        try:
            # Convert to DataFrame
            self.df = pd.DataFrame(self.posts)
            
            # Limit to 200 rows
            self.df = self.df.head(200)
            
            # Add engagement score (points + comments)
            self.df['engagement_score'] = self.df['points'] + self.df['comments']
            
            # Categorize post popularity
            def categorize_popularity(row):
                score = row['engagement_score']
                if score >= 100:
                    return 'viral'
                elif score >= 50:
                    return 'popular'
                elif score >= 20:
                    return 'moderate'
                else:
                    return 'new'
            
            self.df['popularity'] = self.df.apply(categorize_popularity, axis=1)
            
            # Clean URLs - handle relative links
            def clean_url(url):
                if url.startswith('item?id='):
                    return f"https://news.ycombinator.com/{url}"
                return url
            
            self.df['url'] = self.df['url'].apply(clean_url)
            
            # Add flag for external vs internal links
            self.df['is_external'] = ~self.df['url'].str.contains('news.ycombinator.com')
            
            logger.info(f"✅ Transformed {len(self.df)} posts")
            logger.info(f"  Columns: {', '.join(self.df.columns)}")
            
        except Exception as e:
            logger.error(f"❌ Transform failed: {e}")
            raise
    
    def _stage_load(self, stage):
        """Stage 3: Load data to PostgreSQL"""
        logger.info("\n💾 Stage 3: Loading to PostgreSQL")
        
        try:
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
            self.df.to_sql(
                table_name,
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✅ {len(self.df)} rows loaded to {table_name}")
            
            # Create indexes
            if destination.get('create_indexes'):
                index_columns = destination.get('index_columns', [])
                with engine.connect() as conn:
                    for col in index_columns:
                        if col in self.df.columns:
                            index_name = f"idx_{table_name}_{col}"
                            try:
                                conn.execute(text(
                                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col})"
                                ))
                            except Exception as e:
                                logger.warning(f"  ⚠️ Failed to create index on {col}: {e}")
                    conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Load failed: {e}")
            raise

if __name__ == '__main__':
    exit(main())
