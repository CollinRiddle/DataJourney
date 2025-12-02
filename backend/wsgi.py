import os
import sys

# Ensure backend directory is importable when running under WSGI
basedir = os.path.abspath(os.path.dirname(__file__))
if basedir not in sys.path:
	sys.path.insert(0, basedir)

# Expose Flask app as WSGI callable `application`
from app import app as application

# Optional: set production-safe defaults
os.environ.setdefault('FLASK_DEBUG', '0')

# Load environment variables for DB from backend/config/config.env on PythonAnywhere
try:
	from dotenv import load_dotenv
	env_path = os.path.join(basedir, 'config', 'config.env')
	if os.path.exists(env_path):
		load_dotenv(env_path)
except Exception:
	# If python-dotenv isn't available due to disk quota, rely on PA env settings
	pass