# ------------------------------------------------------------------- # 
# Hub for helper functions used across multiple scripts
# ------------------------------------------------------------------- # 

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Retrieve the database URI
database_url = os.getenv("AIVEN_PG_URI")
if not database_url:
    raise ValueError("No database URI found.")

# Ensure proper SQLAlchemy dialect for PostgreSQL
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)

engine = create_engine(database_url)

# ------------------------------------------------------------------- # 
# HELPER FUNCTIONS

def query_to_dataframe(sql_query: str) -> pd.DataFrame:

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()
