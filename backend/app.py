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
import os
import json
from typing import cast

# Get the absolute path to the backend directory
basedir = os.path.abspath(os.path.dirname(__file__))
# Build path to frontend/dist
static_folder = os.path.join(os.path.dirname(basedir), 'frontend', 'dist')

app = Flask(__name__, static_folder=static_folder, static_url_path='')

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