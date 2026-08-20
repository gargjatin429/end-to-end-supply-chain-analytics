import polars as pl
import s3fs
from sqlalchemy import create_engine
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DIM_GEO_PATH, DIM_CUST_PATH, DIM_PROD_PATH, SQL_SERVER_NAME, SQL_DATABASE, SQL_DRIVER, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DIM_PATHS = {
    "Dim_Geo": DIM_GEO_PATH,
    "Dim_Customer_Geo": DIM_CUST_PATH,
    "Dim_Product": DIM_PROD_PATH
}

def main():
    logging.info("Starting dimension load pipeline.")

    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    is_testing = os.getenv("TEST_MODE", "false").lower() == "true"

    if is_testing:
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

    for table_name, file_path in DIM_PATHS.items():
        logging.info(f"Loading dimension table: {table_name}")

        if not fs.exists(file_path.replace("s3://", "")):
            logging.error(f"File not found: {file_path}")
            continue

        try:
            with fs.open(file_path, 'rb') as f:
                df = pl.read_parquet(f)

            logging.info(f"Read {df.height} rows.")

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