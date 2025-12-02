"""
Database Connection Utilities for DataJourney

Provides centralized database connection management for both SQLAlchemy
and psycopg2 connections. Handles URI normalization and SSL configuration
for Aiven PostgreSQL.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2

# Load environment variables from backend/config/config.env
_ENV_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "config", "config.env"))
load_dotenv(_ENV_PATH)

def _normalize_pg_uri(uri: str) -> str:
    """
    Normalize PostgreSQL URI for library compatibility.
    
    Ensures the URI uses 'postgresql://' scheme and includes SSL mode.
    
    Args:
        uri: Database connection URI
        
    Returns:
        Normalized URI string
    """
    if not uri:
        return uri
    
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    
    if "sslmode=" not in uri:
        sep = "&" if "?" in uri else "?"
        uri = f"{uri}{sep}sslmode=require"
    
    return uri


def get_engine():
    """
    Create and return a SQLAlchemy engine for database connections.
    
    Returns:
        SQLAlchemy Engine instance
        
    Raises:
        ValueError: If database configuration is missing
    """
    uri = os.getenv("AIVEN_PG_URI") or os.getenv("DATABASE_URL")
    if uri:
        sa_uri = _normalize_pg_uri(uri).replace("postgresql://", "postgresql+psycopg2://", 1)
        return create_engine(sa_uri)

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
    """
    Create and return a psycopg2 database connection.
    
    Returns:
        psycopg2 connection object
        
    Raises:
        ValueError: If database configuration is missing
    """
    uri = os.getenv("AIVEN_PG_URI") or os.getenv("DATABASE_URL")
    
    if not uri:
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