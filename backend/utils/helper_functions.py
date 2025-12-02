"""
Helper Functions for DataJourney Pipelines

Provides utility functions for common database operations used across
multiple pipeline scripts.
"""

import pandas as pd
from sqlalchemy import text
from .connection import get_engine


def query_to_dataframe(sql_query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame.
    
    Args:
        sql_query: SQL query string to execute
        
    Returns:
        DataFrame containing query results, or empty DataFrame on error
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()
