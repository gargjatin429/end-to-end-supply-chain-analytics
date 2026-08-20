import polars as pl
from sqlalchemy import create_engine
import logging
import sys
import os

# Add the project root to the python path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DIM_GEO_PATH, DIM_CUST_PATH, DIM_PROD_PATH, SQL_SERVER_NAME, SQL_DATABASE, SQL_DRIVER, ensure_directories

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dimension table sources (Silver Layer)
DIM_PATHS = {
    "Dim_Geo": DIM_GEO_PATH,
    "Dim_Customer_Geo": DIM_CUST_PATH,
    "Dim_Product": DIM_PROD_PATH
}

def main():
    ensure_directories()
    logging.info("Starting dimension load pipeline.")

    # --------------------------------------------------------------------------
    # STEP 1: CONNECT TO SQL SERVER
    # --------------------------------------------------------------------------
    is_testing = os.getenv("TEST_MODE", "false").lower() == "true"

    if is_testing:
        # Use a local SQLite database for testing instead of SQL Server
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test_analytics.db")
        connection_string = f"sqlite:///{db_path}"
        logging.info(f"Running in test mode. Using SQLite at {db_path}")
    else:
        connection_string = (
            f"mssql+pyodbc://@{SQL_SERVER_NAME}/{SQL_DATABASE}"
            f"?driver={SQL_DRIVER}&trusted_connection=yes"
        )

    try:
        if is_testing:
            engine = create_engine(connection_string)
        else:
            engine = create_engine(connection_string, fast_executemany=True)

        with engine.connect():
            pass
        logging.info("Connected to database.")
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        return

    # --------------------------------------------------------------------------
    # STEP 2: LOAD DIMENSION TABLES
    # --------------------------------------------------------------------------
    for table_name, file_path in DIM_PATHS.items():
        logging.info(f"Loading dimension table: {table_name}")

        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            continue

        try:
            # Read Parquet from Silver layer
            df = pl.read_parquet(file_path)
            logging.info(f"Read {df.height} rows.")

            # Append to SQL Server table
            df.to_pandas().to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            logging.info(f"Loaded {table_name} successfully.")

        except Exception as e:
            logging.error(f"Error loading {table_name}: {e}", exc_info=True)
            logging.info("Skipping this dimension.")

    logging.info("Dimension loading complete.")

if __name__ == "__main__":
    main()
