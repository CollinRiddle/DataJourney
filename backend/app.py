from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import os
import sys
import json

# Get the absolute path to the backend directory
basedir = os.path.abspath(os.path.dirname(__file__))
# Build path to frontend/dist
static_folder = os.path.join(os.path.dirname(basedir), 'frontend', 'dist')

# Ensure backend package path is importable in hosted environments
if basedir not in sys.path:
    sys.path.insert(0, basedir)

app = Flask(__name__, static_folder=static_folder, static_url_path='')
CORS(app)  # Enable CORS for all routes

@app.route('/')
def serve_react():
    """Serve the React app"""
    try:
        assert app.static_folder is not None
        return send_from_directory(app.static_folder, 'index.html')
    except Exception as e:
        return f"Error serving index.html: {str(e)}", 500

# Serve static assets (JS, CSS, images, etc.)
@app.route('/assets/<path:filename>')
def serve_assets(filename):
    """Serve files from the assets directory"""
    assert app.static_folder is not None
    assets_dir = os.path.join(app.static_folder, 'assets')
    return send_from_directory(assets_dir, filename)

@app.route('/vite.svg')
def serve_vite_svg():
    """Serve the vite.svg file"""
    assert app.static_folder is not None
    return send_from_directory(app.static_folder, 'vite.svg')

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
    """Serve pre-exported data from JSON files"""
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

    data_file = os.path.join(basedir, 'data_exports', f'{pipeline_id}.json')

    if os.path.exists(data_file):
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            response = jsonify({
                "pipeline_id": pipeline_id,
                "table_name": table_name,
                "row_count": len(data),
                "data": data
            })
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            return response
        except Exception as e:
            return jsonify({"error": f"Error reading data file: {str(e)}"}), 500
    else:
        return jsonify({
            "error": f"Data file not found for {pipeline_id}",
            "data": []
        }), 404

@app.route('/api/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    debug_flag = os.getenv('FLASK_DEBUG', '0') == '1'
    app.run(host='127.0.0.1', port=5000, debug=debug_flag)

# Catch-all route for client-side routing (must be last)
@app.route('/<path:path>')
def catch_all(path):
    """Catch-all for client-side routing"""
    # If the file exists in static folder, serve it
    assert app.static_folder is not None
    file_path = os.path.join(app.static_folder, path)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return send_from_directory(app.static_folder, path)
    # Otherwise, serve index.html for client-side routing
    return send_from_directory(app.static_folder, 'index.html')

# JSON error handlers for API routes to prevent HTML error pages
@app.errorhandler(404)
def handle_404(err):
    try:
        if request.path.startswith('/api/'):
            return jsonify({"error": "Not found", "path": request.path}), 404
    except Exception:
        pass
    return "Not Found", 404

@app.errorhandler(500)
def handle_500(err):
    try:
        if request.path.startswith('/api/'):
            return jsonify({"error": "Internal server error"}), 500
    except Exception:
        pass
    return "Internal Server Error", 500