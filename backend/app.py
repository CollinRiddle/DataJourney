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

# Example API route to return your pipeline JSONs
@app.route('/api/pipelines')
def get_pipelines():
    data_dir = os.path.join(os.path.dirname(__file__), 'data_config')
    pipelines = []

    for file in os.listdir(data_dir):
        if file.endswith('.json'):
            with open(os.path.join(data_dir, file)) as f:
                pipelines.append(json.load(f))

    return jsonify(pipelines)

if __name__ == '__main__':
    app.run(debug=True)
