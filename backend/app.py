from flask import Flask, jsonify, send_from_directory
import os
import json
from typing import cast

# app = Flask(__name__, static_folder='../frontend/dist', static_url_path='')

# # Serve React build (for production)
# @app.route('/')
# def serve_react():
#     # Ensure static_folder is treated as a non-None str for the type checker
#     directory = cast(str, app.static_folder)
#     return send_from_directory(directory, 'index.html')

# # API route to return pipeline configurations
# @app.route('/api/pipelines')
# def get_pipelines():
#     data_dir = os.path.join(os.path.dirname(__file__), 'data_config')
#     config_file = os.path.join(data_dir, 'pipeline_config.json')
    
#     try:
#         with open(config_file, 'r') as f:
#             config = json.load(f)
        
#         # Return just the pipelines array from the config
#         return jsonify(config.get('pipelines', []))
    
#     except FileNotFoundError:
#         return jsonify({"error": "pipeline_config.json not found"}), 404
#     except json.JSONDecodeError as e:
#         return jsonify({"error": f"Invalid JSON: {str(e)}"}), 400
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True)

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import os
import json
from typing import cast
import pandas as pd
import numpy as np

# Get the absolute path to the backend directory
basedir = os.path.abspath(os.path.dirname(__file__))
# Build path to frontend/dist
static_folder = os.path.join(os.path.dirname(basedir), 'frontend', 'dist')

app = Flask(__name__, static_folder=static_folder, static_url_path='')
CORS(app)  # Enable CORS for all routes

@app.route('/')
def serve_react():
    """Serve the React app"""
    try:
        return send_from_directory(app.static_folder, 'index.html')
    except Exception as e:
        return f"Error serving index.html: {str(e)}", 500

# Serve static assets (JS, CSS, images, etc.)
@app.route('/assets/<path:filename>')
def serve_assets(filename):
    """Serve files from the assets directory"""
    assets_dir = os.path.join(app.static_folder, 'assets')
    return send_from_directory(assets_dir, filename)

@app.route('/vite.svg')
def serve_vite_svg():
    """Serve the vite.svg file"""
    return send_from_directory(app.static_folder, 'vite.svg')

# Catch-all route for client-side routing (must be last)
@app.route('/<path:path>')
def catch_all(path):
    """Catch-all for client-side routing"""
    # If the file exists in static folder, serve it
    file_path = os.path.join(app.static_folder, path)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return send_from_directory(app.static_folder, path)
    # Otherwise, serve index.html for client-side routing
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/api/pipelines')
def get_pipelines():
    """API route to return pipeline configurations"""
    data_dir = os.path.join(basedir, 'data_config')
    config_file = os.path.join(data_dir, 'pipeline_config.json')
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Return just the pipelines array from the config
        return jsonify(config.get('pipelines', []))
    
    except FileNotFoundError:
        return jsonify({"error": "pipeline_config.json not found"}), 404
    except json.JSONDecodeError as e:
        return jsonify({"error": f"Invalid JSON: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/pipelines/<pipeline_id>/data')
def get_pipeline_data(pipeline_id):
    """API route to fetch actual data from PostgreSQL for a specific pipeline"""
    from utils.connection import get_connection
    
    # Map pipeline IDs to their database table names
    pipeline_table_map = {
        'thailand_hotels': 'hotel_listings',
        'pokemon_data': 'pokemon_data',
        'spacex_launches': 'spacex_launch_analytics',
        'weather_analytics': 'weather_analytics',
        'hackernews_scraper': 'hackernews_posts',
        'network_traffic': 'network_traffic_analysis',
        'stock_market': 'stock_market_analytics'
    }
    
    table_name = pipeline_table_map.get(pipeline_id)
    if not table_name:
        return jsonify({"error": f"Unknown pipeline_id: {pipeline_id}"}), 404
    
    try:
        # Connect to PostgreSQL and fetch data
        conn = get_connection()
        query = f"SELECT * FROM {table_name} LIMIT 200"
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Normalize values to be JSON-safe
        # - Replace NaN/NaT with None (becomes null in JSON)
        # - Replace +/-inf with None
        # Use both replace and where to cover numeric and datetime types robustly
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df = df.where(pd.notnull(df), None)
        
        # Convert DataFrame to list of dictionaries
        data = df.to_dict('records')
        
        response = jsonify({
            "pipeline_id": pipeline_id,
            "table_name": table_name,
            "row_count": len(data),
            "data": data
        })
        # Prevent browser/proxy caching so re-runs are visible immediately
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        return response
    
    except Exception as e:
        error_msg = str(e)
        # Check if table doesn't exist
        if 'does not exist' in error_msg.lower():
            return jsonify({
                "error": f"Table '{table_name}' does not exist. Please run the pipeline first.",
                "pipeline_id": pipeline_id,
                "table_name": table_name,
                "data": []
            }), 404
        else:
            return jsonify({
                "error": f"Database error: {error_msg}",
                "pipeline_id": pipeline_id,
                "data": []
            }), 500

# Debug route to check paths
@app.route('/api/debug')
def debug_info():
    """Debug route to verify paths"""
    return jsonify({
        "basedir": basedir,
        "static_folder": app.static_folder,
        "static_folder_exists": os.path.exists(app.static_folder) if app.static_folder else False,
        "index_exists": os.path.exists(os.path.join(app.static_folder, 'index.html')) if app.static_folder else False,
        "static_contents": os.listdir(app.static_folder) if app.static_folder and os.path.exists(app.static_folder) else []
    })

if __name__ == '__main__':
    app.run(debug=True)