import pandas as pd
from datetime import datetime
from pathlib import Path
from src.utils.db import get_engine
from sqlalchemy import text

BASE_PATH = Path("data/raw")

def add_missing_columns(conn, table_name, schema, df):
    """
    Cek kolom yang ada di df tapi belum ada di table,
    lalu ALTER TABLE untuk tambahkan kolom baru.
    """
    existing_cols = conn.execute(text(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table_name}'
    """)).fetchall()
    
    existing_col_names = {row[0] for row in existing_cols}
    
    for col in df.columns:
        if col not in existing_col_names:
            conn.execute(text(
                f'ALTER TABLE {schema}.{table_name} ADD COLUMN "{col}" TEXT'
            ))
            print(f"  [SCHEMA EVOLUTION] Added column '{col}' to {schema}.{table_name}")

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
    ingestion_time = datetime.now()

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
                    col_name, col_val = part.split("=", 1)
                    df[col_name] = col_val

            # Add Audit Columns
            df['_loaded_at'] = ingestion_time
            df['_source_file'] = str(file_path.relative_to(BASE_PATH))

            # Load to Postgres
            with engine.begin() as conn:

                if strategy == 'append':
                    add_missing_columns(conn, table_name, 'raw', df)
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema='raw',
                    if_exists=strategy,
                    index=False,
                    chunksize=1000,
                    method='multi'
                )
            
            print(f"  {strategy.upper()}: {len(df)} rows to raw.{table_name} (from {file_path.name})")

        except Exception as e:
            print(f"  Error processing {file_path}: {e}")

    print("\n All folders have been consolidated into unified tables!")

if __name__ == "__main__":
    ingest_data()