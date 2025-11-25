# ------------------------------------------------------------------- #
# Network Traffic Anomaly Detection Pipeline
# Intermediate ETL with Security Analytics
#
# DATA LIMIT: 200 network traffic records
#
# Pipeline Flow:
# 1. Extract from Kaggle using KaggleHub
# 2. Analyze Traffic Patterns & Classify Security Threats
# 3. Calculate Risk Scores & Anomaly Metrics
# 4. Load to PostgreSQL
# ------------------------------------------------------------------- #

import os
import logging
from datetime import datetime

import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -------------------------------------------------------------------------- #
# Main function to run the pipeline

def main():
    try:
        pipeline = NetworkTrafficPipeline()
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

class NetworkTrafficPipeline:
    """
    Intermediate pipeline analyzing network traffic for anomaly detection.
    Extracts data from Kaggle, performs security analytics, and loads to PostgreSQL.
    """
    
    def __init__(self):
        """Initialize pipeline with configuration"""
        logger.info("🔄 Initializing Network Traffic Anomaly Detection Pipeline")
        
        self.df = None
        
        # Load pipeline configuration
        import json
        config_path = os.path.join(os.path.dirname(__file__), '..', 'data_config', 'pipeline_config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Find this pipeline's config
        self.config = None
        for pipeline in config['pipelines']:
            if pipeline['pipeline_id'] == 'network_traffic':
                self.config = pipeline
                break
        
        if not self.config:
            raise ValueError("Pipeline configuration not found for 'network_traffic'")
        
        logger.info("✅ Pipeline initialized")
    
    def run(self):
        """Execute the complete pipeline"""
        try:
            logger.info("\n" + "="*60)
            logger.info("Starting Network Traffic Anomaly Detection Pipeline")
            logger.info("="*60)
            
            # Execute each stage
            for stage in self.config['stages']:
                stage_id = stage['stage_id']
                
                if stage_id == 'extract_kaggle_traffic':
                    self._stage_extract()
                elif stage_id == 'analyze_traffic_patterns':
                    self._stage_analyze()
                elif stage_id == 'calculate_risk_scores':
                    self._stage_risk_scoring()
                elif stage_id == 'load_traffic_data':
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
    
    def _stage_extract(self):
        """Stage 1: Extract network traffic data from Kaggle"""
        logger.info("\n📥 Stage 1: Extracting from Kaggle")
        
        try:
            # Download dataset using kagglehub
            logger.info("  Downloading dataset from Kaggle...")
            df = kagglehub.load_dataset(
                KaggleDatasetAdapter.PANDAS,
                "ziya07/network-traffic-anomaly-detection-dataset",
                "embedded_system_network_security_dataset.csv",
            )
            
            logger.info(f"  Dataset downloaded: {len(df)} total records")
            logger.info(f"  Columns: {', '.join(df.columns)}")
            
            # Limit to 200 rows
            self.df = df.head(200)
            
            logger.info(f"✅ Extracted {len(self.df)} network traffic records")
            
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            raise
    
    def _stage_analyze(self):
        """Stage 2: Analyze traffic patterns and classify threats"""
        logger.info("\n🔍 Stage 2: Analyzing traffic patterns")
        
        try:
            # Analyze protocol type (already one-hot encoded)
            protocol_cols = [col for col in self.df.columns if col.startswith('protocol_type_')]
            if protocol_cols:
                # Determine primary protocol for each row
                self.df['primary_protocol'] = 'unknown'
                for col in protocol_cols:
                    protocol = col.replace('protocol_type_', '').lower()
                    self.df.loc[self.df[col] == 1, 'primary_protocol'] = protocol
            
            # Classify packet sizes
            if 'packet_size' in self.df.columns:
                self.df['packet_category'] = pd.cut(
                    self.df['packet_size'],
                    bins=[0, 100, 500, 1500, float('inf')],
                    labels=['tiny', 'small', 'medium', 'large']
                )
            
            # Detect suspicious ports (common attack vectors)
            if 'src_port' in self.df.columns and 'dst_port' in self.df.columns:
                suspicious_ports = [23, 135, 139, 445, 1433, 3389, 5900]  # Telnet, RPC, SMB, RDP, VNC
                self.df['uses_suspicious_port'] = (
                    self.df['src_port'].isin(suspicious_ports) | 
                    self.df['dst_port'].isin(suspicious_ports)
                )
            
            # Analyze traffic patterns based on packet counts
            if 'packet_count_5s' in self.df.columns:
                # High volume traffic (potential DDoS indicator)
                packet_threshold = self.df['packet_count_5s'].quantile(0.85)
                self.df['high_volume_traffic'] = self.df['packet_count_5s'] > packet_threshold
            
            # Detect SYN flood patterns (TCP flags)
            if 'tcp_flags_SYN' in self.df.columns and 'tcp_flags_SYN-ACK' in self.df.columns:
                # SYN without SYN-ACK may indicate SYN flood attack
                self.df['potential_syn_flood'] = (
                    (self.df['tcp_flags_SYN'] == 1) & 
                    (self.df['tcp_flags_SYN-ACK'] == 0)
                )
            
            # Spectral entropy analysis (low entropy = repetitive/anomalous)
            if 'spectral_entropy' in self.df.columns:
                entropy_threshold = self.df['spectral_entropy'].quantile(0.25)
                self.df['low_entropy_traffic'] = self.df['spectral_entropy'] < entropy_threshold
            
            # Classify based on actual label if present
            if 'label' in self.df.columns:
                self.df['labeled_threat'] = self.df['label'].apply(
                    lambda x: 'malicious' if x == 1 else 'benign'
                )
            
            logger.info(f"✅ Traffic patterns analyzed")
            if 'primary_protocol' in self.df.columns:
                logger.info(f"  Protocol distribution: {self.df['primary_protocol'].value_counts().to_dict()}")
            if 'uses_suspicious_port' in self.df.columns:
                logger.info(f"  Suspicious ports detected: {self.df['uses_suspicious_port'].sum()}")
            if 'labeled_threat' in self.df.columns:
                logger.info(f"  Labeled threats: {self.df['labeled_threat'].value_counts().to_dict()}")
            
        except Exception as e:
            logger.error(f"❌ Analysis failed: {e}")
            raise
    
    def _stage_risk_scoring(self):
        """Stage 3: Calculate risk scores and anomaly metrics"""
        logger.info("\n⚖️ Stage 3: Calculating risk scores")
        
        try:
            # Initialize risk score
            self.df['risk_score'] = 0.0
            
            # Risk Factor 1: Labeled threat (highest weight - from actual dataset label)
            if 'labeled_threat' in self.df.columns:
                self.df.loc[self.df['labeled_threat'] == 'malicious', 'risk_score'] += 50
            
            # Risk Factor 2: Suspicious ports
            if 'uses_suspicious_port' in self.df.columns:
                self.df.loc[self.df['uses_suspicious_port'], 'risk_score'] += 20
            
            # Risk Factor 3: High volume traffic
            if 'high_volume_traffic' in self.df.columns:
                self.df.loc[self.df['high_volume_traffic'], 'risk_score'] += 15
            
            # Risk Factor 4: Low entropy (repetitive patterns)
            if 'low_entropy_traffic' in self.df.columns:
                self.df.loc[self.df['low_entropy_traffic'], 'risk_score'] += 10
            
            # Risk Factor 5: Potential SYN flood
            if 'potential_syn_flood' in self.df.columns:
                self.df.loc[self.df['potential_syn_flood'], 'risk_score'] += 15
            
            # Risk Factor 6: Spectral features (advanced anomaly detection)
            if 'spectral_entropy' in self.df.columns and 'frequency_band_energy' in self.df.columns:
                # Very low entropy combined with high energy suggests attack pattern
                entropy_low = self.df['spectral_entropy'] < self.df['spectral_entropy'].quantile(0.15)
                energy_high = self.df['frequency_band_energy'] > self.df['frequency_band_energy'].quantile(0.85)
                self.df.loc[entropy_low & energy_high, 'risk_score'] += 10
            
            # Risk Factor 7: Unusual packet sizes
            if 'packet_size' in self.df.columns:
                # Very small packets (potential port scanning)
                self.df.loc[self.df['packet_size'] < 50, 'risk_score'] += 8
                # Very large packets (potential DDoS or data exfiltration)
                self.df.loc[self.df['packet_size'] > 1400, 'risk_score'] += 6
            
            # Categorize overall threat level
            self.df['threat_level'] = pd.cut(
                self.df['risk_score'],
                bins=[-1, 10, 30, 60, 100],
                labels=['low', 'medium', 'high', 'critical']
            )
            
            # Calculate anomaly confidence (0-100%)
            max_risk = self.df['risk_score'].max()
            if max_risk > 0:
                self.df['anomaly_confidence'] = (self.df['risk_score'] / max_risk * 100).round(2)
            else:
                self.df['anomaly_confidence'] = 0.0
            
            # Flag for investigation (high/critical threats)
            self.df['requires_investigation'] = self.df['threat_level'].isin(['high', 'critical'])
            
            # Add timestamp for processing
            self.df['processed_at'] = pd.Timestamp.now()
            
            logger.info(f"✅ Risk scores calculated")
            logger.info(f"  Threat distribution: {self.df['threat_level'].value_counts().to_dict()}")
            logger.info(f"  Records requiring investigation: {self.df['requires_investigation'].sum()}")
            logger.info(f"  Average risk score: {self.df['risk_score'].mean():.2f}")
            logger.info(f"  Max risk score: {self.df['risk_score'].max():.2f}")
            
        except Exception as e:
            logger.error(f"❌ Risk scoring failed: {e}")
            raise
    
    def _stage_load(self, stage):
        """Stage 4: Load processed traffic data to PostgreSQL"""
        logger.info("\n💾 Stage 4: Loading to PostgreSQL")
        
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
    
    # Helper methods
    
    def _classify_protocol(self, protocol):
        """Classify network protocol into categories"""
        protocol = str(protocol).upper()
        if protocol in ['TCP', '6']:
            return 'tcp'
        elif protocol in ['UDP', '17']:
            return 'udp'
        elif protocol in ['ICMP', '1']:
            return 'icmp'
        elif protocol in ['HTTP', 'HTTPS']:
            return 'http'
        else:
            return 'other'
    
    def _is_internal_ip(self, ip):
        """Check if IP is in private ranges (10.x, 172.16-31.x, 192.168.x)"""
        try:
            ip_str = str(ip)
            parts = ip_str.split('.')
            if len(parts) != 4:
                return False
            
            first = int(parts[0])
            second = int(parts[1])
            
            # Private IP ranges
            if first == 10:
                return True
            if first == 172 and 16 <= second <= 31:
                return True
            if first == 192 and second == 168:
                return True
            
            return False
        except:
            return False

if __name__ == '__main__':
    exit(main())
