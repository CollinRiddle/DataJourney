# Author: Collin Riddle

import sys
from pathlib import Path

# --------------------------------------------------------------------------------------- # 
# Add project root to Python path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# --------------------------------------------------------------------------------------- # 
# IMPORTS

import pandas as pd
from sqlalchemy import text
from utils.connection import get_engine, get_connection
import kagglehub
import os

# --------------------------------------------------------------------------------------- # 

def main():

    run_pipeline()
    # test_db()

# --------------------------------------------------------------------------------------- # 

def test_db():

    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT COLUMN_NAME,* FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'raw_resorts_thailand' AND TABLE_SCHEMA='public';")
    rows = cur.fetchall()

    for row in rows:
        print(row)

    cur.close()
    conn.close()

# --------------------------------------------------------------------------------------- # 

def explore_dataset(path):

    # List all files in the directory
    files = os.listdir(path)
    print(f"Files found in dataset: {files}\n")
    
    # Find the CSV file
    csv_files = [f for f in files if f.endswith('.csv')]
    
    if csv_files:
        csv_file = csv_files[0]
        file_path = os.path.join(path, csv_file)
        print(f"Reading file: {csv_file}\n")
        
        df = pd.read_csv(file_path)
        return df
    else:
        print("No CSV files found!")
        return None

# --------------------------------------------------------------------------------------- # 

def run_pipeline():

    # Download dataset using KaggleHub
    path = kagglehub.dataset_download("aakashshinde1507/resorts-in-thailand")
    # Get column names

    raw_df = explore_dataset(path)

    print("Column names:")
    print(raw_df.columns.tolist())

    # Or see first few rows with column names
    print("\nFirst 5 rows:")
    print(raw_df.head())
    
    # if raw_df is not None:
    #     try:
    #         engine = get_engine()

    #         # Test the connection
    #         with engine.connect() as conn:
    #             print("Database connection successful!")
            
    #         # Loading - Insert raw data into database
    #         table_name = "raw_resorts_thailand"
            
    #         raw_df.to_sql(table_name, engine, if_exists="replace", index=False)

    #         print(f"\nData inserted into table '{table_name}' successfully!")
            
    #     except Exception as e:
    #         print(f"\n Database connection failed!")
    #         print(f"Error type: {type(e).__name__}")
    #         print(f"Error message: {str(e)}")
            
    # else:
    #     print("Error: Could not load dataset")

# --------------------------------------------------------------------------------------- # 

if __name__ == "__main__":
    main()