"""
Data Export Script for DataJourney

Exports data from PostgreSQL database tables to static JSON files for deployment.
This script should be run locally (where database access is available) before
deploying to PythonAnywhere or other hosting platforms with connection restrictions.

Usage:
    python backend/export_data.py

Output:
    Creates/updates JSON files in backend/data_exports/ directory
    Each file contains up to 200 rows per pipeline
"""

import json
import os
from utils.connection import get_connection
from decimal import Decimal

# Map pipeline IDs to their corresponding database table names
pipeline_table_map = {
    'thailand_hotels': 'hotel_listings',
    'pokemon_data': 'pokemon_data',
    'spacex_launches': 'spacex_launch_analytics',
    'weather_analytics': 'weather_analytics',
    'hackernews_scraper': 'hackernews_posts',
    'network_traffic': 'network_traffic_analysis',
    'stock_market': 'stock_market_analytics',
    'crypto_market': 'crypto_market_analysis',
    'csv_kaggle': 'customer_shopping_data'
}

# Create output directory if it doesn't exist
output_dir = os.path.join(os.path.dirname(__file__), 'data_exports')
os.makedirs(output_dir, exist_ok=True)

print("Starting data export...\n")

try:
    conn = get_connection()
    
    for pipeline_id, table_name in pipeline_table_map.items():
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 200")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            # Convert rows to JSON-serializable format
            data = []
            for row in rows:
                record = {}
                for col, val in zip(columns, row):
                    # Handle datetime objects
                    if hasattr(val, 'isoformat'):
                        record[col] = val.isoformat()
                    # Handle Decimal objects
                    elif isinstance(val, Decimal):
                        record[col] = float(val)
                    else:
                        record[col] = val
                data.append(record)
            
            # Write to JSON file
            output_file = os.path.join(output_dir, f'{pipeline_id}.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            
            print(f"✓ Exported {len(data)} rows for {pipeline_id}")
            cursor.close()
            
        except Exception as e:
            print(f"✗ Failed to export {pipeline_id}: {e}")
            continue
    
    conn.close()
    print("\n✅ All data exported successfully!")
    
except Exception as e:
    print(f"\n❌ Export failed: {e}")
    raise
