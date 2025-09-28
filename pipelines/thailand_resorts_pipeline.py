# Author: Collin Riddle

import pandas as pd
from sqlalchemy import text
from utils.connection import get_engine    

def clean_data(df):
    pass

def run_pipeline():
    engine = get_engine()

    # Extraction
    raw_df = pd.read_csv("../data/sample_data.csv")  # or read from API/DB

    # Transformation
    cleaned_df = clean_data(raw_df)

    # Loading
    cleaned_df.to_sql("production_table", engine, if_exists="replace", index=False)

    print("Pipeline executed successfully!")

if __name__ == "__main__":
    run_pipeline()