import json
import os
from utils.connection import get_connection
from decimal import Decimal

pipeline_table_map = {
    'thailand_hotels': 'hotel_listings',
    'pokemon_data': 'pokemon_data',
    'spacex_launches': 'spacex_launch_analytics',
    'weather_analytics': 'weather_analytics',
    'hackernews_scraper': 'hackernews_posts',
    'network_traffic': 'network_traffic_analysis',
    'stock_market': 'stock_market_analytics'
}

output_dir = os.path.join(os.path.dirname(__file__), 'data_exports')
os.makedirs(output_dir, exist_ok=True)

conn = get_connection()
for pipeline_id, table_name in pipeline_table_map.items():
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 200")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    data = []
    for row in rows:
        record = {}
        for col, val in zip(columns, row):
            if hasattr(val, 'isoformat'):
                record[col] = val.isoformat()
            elif isinstance(val, Decimal):
                record[col] = float(val)
            else:
                record[col] = val
        data.append(record)
    output_file = os.path.join(output_dir, f'{pipeline_id}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"✓ Exported {len(data)} rows for {pipeline_id}")
    cursor.close()
conn.close()
print("All data exported!")
