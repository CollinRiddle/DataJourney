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