import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2

# Load environment variables from backend/config/config.env reliably
_ENV_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "config", "config.env"))
load_dotenv(_ENV_PATH)

def _normalize_pg_uri(uri: str) -> str:
    """Ensure URI uses a scheme acceptable by libraries and includes sslmode."""
    if not uri:
        return uri
    # psycopg2 accepts postgresql://; SQLAlchemy prefers postgresql+psycopg2://
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    # Ensure sslmode=require is present for Aiven
    if "sslmode=" not in uri:
        sep = "&" if "?" in uri else "?"
        uri = f"{uri}{sep}sslmode=require"
    return uri

def get_engine():
    # Prefer full URI if provided
    uri = os.getenv("AIVEN_PG_URI") or os.getenv("DATABASE_URL")
    if uri:
        sa_uri = _normalize_pg_uri(uri).replace("postgresql://", "postgresql+psycopg2://", 1)
        return create_engine(sa_uri)

    # Fallback to individual components
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")
    if not all([user, password, host, port, dbname]):
        raise ValueError("Database configuration missing. Set AIVEN_PG_URI or DB_* variables.")
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
    return create_engine(conn_str)

def get_connection():
    uri = os.getenv("AIVEN_PG_URI") or os.getenv("DATABASE_URL")
    if not uri:
        # Fallback to building a DSN
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASS")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        dbname = os.getenv("DB_NAME")
        if not all([user, password, host, port, dbname]):
            raise ValueError("Database configuration missing. Set AIVEN_PG_URI or DB_* variables.")
        uri = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
    uri = _normalize_pg_uri(uri)
    return psycopg2.connect(uri)