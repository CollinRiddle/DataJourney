from flask import Flask, jsonify, send_from_directory
import os
import json
from typing import cast

app = Flask(__name__, static_folder='../frontend/dist', static_url_path='')

# Serve React build (for production)
@app.route('/')
def serve_react():
    # Ensure static_folder is treated as a non-None str for the type checker
    directory = cast(str, app.static_folder)
    return send_from_directory(directory, 'index.html')

# API route to return pipeline configurations
@app.route('/api/pipelines')
def get_pipelines():
    data_dir = os.path.join(os.path.dirname(__file__), 'data_config')
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

if __name__ == '__main__':
    app.run(debug=True)