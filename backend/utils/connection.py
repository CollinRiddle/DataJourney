import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from config.env with absolute path
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.env')
load_dotenv(config_path)

# NOTE: We avoid heavy dependencies (like SQLAlchemy/Pandas) for PythonAnywhere free tier.
# Use lightweight psycopg2 connections for API preview endpoints.

def get_connection():
    uri = os.getenv("AIVEN_PG_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise RuntimeError("Database URI not set (AIVEN_PG_URI or DATABASE_URL)")
    # Normalize postgres:// to postgresql:// for compatibility
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    conn = psycopg2.connect(uri)
    return conn