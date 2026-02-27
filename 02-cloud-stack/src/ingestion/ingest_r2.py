import os
import logging
from dotenv import load_dotenv
from snowflake.connector import connect

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# ── CONFIGURATION ────────────────────────────────────────
SNOWFLAKE_DB     = "DATA_PLATFORM"
SNOWFLAKE_SCHEMA = "RAW"
STAGE_NAME       = "stg_r2_y2"

DATASETS = [
    "subscription_events",
    "payments",
    "product_events",
    "users",
]


def get_snowflake_conn():
    return connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )


def run_ingestion():
    conn   = get_snowflake_conn()
    cursor = conn.cursor()

    for dataset in DATASETS:
        try:
            table_name = dataset.upper()
            logger.info(f"Starting ingestion for: {table_name}")

            # STEP 1: Create raw table with VARIANT + audit columns
            logger.info(f"  [1/3] Creating table...")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name} (
                    raw            VARIANT,
                    _stg_file_name VARCHAR,
                    _stg_loaded_at TIMESTAMP_NTZ
                )
            """)

            # STEP 2: COPY INTO — land everything as VARIANT + capture filename + timestamp
            logger.info(f"  [2/3] Executing COPY INTO (cloud-to-cloud)...")
            cursor.execute(f"""
                COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name}
                FROM (
                    SELECT
                        $1,
                        METADATA$FILENAME,
                        CURRENT_TIMESTAMP()
                    FROM @{STAGE_NAME}/{dataset}/
                )
                FILE_FORMAT = (FORMAT_NAME = 'fmt_parquet')
                FORCE = FALSE
                PURGE = FALSE
            """)

            results   = cursor.fetchall()
            total_rows = sum(r[3] for r in results if r[3])
            logger.info(f"  [3/3] Done! Files: {len(results)} | Rows loaded: {total_rows}\n")

        except Exception as e:
            logger.error(f"  FAILED [{dataset}]: {str(e)}\n")
            continue

    cursor.close()
    conn.close()
    logger.info("All processes finished.")


if __name__ == "__main__":
    run_ingestion()