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
    # Fallback: build URI from discrete DB_* settings if present
    if not uri:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        dbname = os.getenv("DB_NAME")
        if all([user, password, host, port, dbname]):
            uri = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
    if not uri:
        raise RuntimeError("Database URI not set (AIVEN_PG_URI or DATABASE_URL)")
    # Normalize postgres:// to postgresql:// for compatibility
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    conn = psycopg2.connect(uri)
    return conn