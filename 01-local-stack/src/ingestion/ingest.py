import pandas as pd
from datetime import datetime
from pathlib import Path
from src.utils.db import get_engine

BASE_PATH = Path("data/raw")

def ingest_data():
    engine = get_engine()
    # Search for all parquet files under the BASE_PATH (including subfolders)
    parquet_files = sorted(BASE_PATH.rglob("*.parquet"))

    if not parquet_files:
        print(f" No parquet files found in {BASE_PATH.absolute()}")
        return

    print(f" Found {len(parquet_files)} files. Starting unified ingestion...")

    # Set for tracking which tables have been reset (replaced) to avoid multiple replacements
    tables_reset = set()

    for file_path in parquet_files:
        
        table_name = file_path.relative_to(BASE_PATH).parts[0]
        
        if table_name not in tables_reset:
            strategy = 'replace'
            tables_reset.add(table_name)
        else:
            strategy = 'append'

        try:
            df = pd.read_parquet(file_path, engine="pyarrow")
            
            # --- HANDLING HIVE PARTITION DATA ---
            # If the file is stored in a partitioned folder structure (like event_date=2024-01-01), 
            # Pick up the partition info and add it as columns to the DataFrame
            for part in file_path.parts:
                if "=" in part:
                    col_name, col_val = part.split("=")
                    df[col_name] = col_val

            # Add Audit Columns
            df['_loaded_at'] = datetime.now()
            df['_source_file'] = str(file_path.relative_to(BASE_PATH))

            # Load to Postgres
            with engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema='raw',
                    if_exists=strategy,
                    index=False,
                    chunksize=10000,
                    method='multi'
                )
            
            print(f"  {strategy.upper()}: {len(df)} rows to raw.{table_name} (from {file_path.name})")

        except Exception as e:
            print(f"  Error processing {file_path}: {e}")

    print("\n All folders have been consolidated into unified tables!")

if __name__ == "__main__":
    ingest_data()