import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2

load_dotenv("config/config.env")

def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
    return create_engine(conn_str)

def get_connection():
    uri = os.getenv("AIVEN_PG_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise RuntimeError("Database URI not set (AIVEN_PG_URI or DATABASE_URL)")
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    conn = psycopg2.connect(uri)
    return conn