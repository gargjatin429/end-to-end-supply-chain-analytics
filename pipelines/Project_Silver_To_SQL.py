import polars as pl
import s3fs
import os
import logging
from sqlalchemy import create_engine
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SILVER_DIR, ARCHIVE_DIR, SQL_SERVER_NAME, SQL_DATABASE, SQL_DRIVER, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TABLE_NAME = "Fact_Sales"

STRICT_COLUMNS = [
    "geo_id", "customer_geo_id", "product_key",
    "order_year", "order_month", "order_day",
    "day_name_str", "order_day_type",
    "type", "days_for_shipping_real", "days_for_shipment_scheduled",
    "shipping_delta", "delivery_class", "shipping_mode_clean",
    "order_status", "customer_segment",
    "order_item_quantity", "order_item_product_price",
    "order_item_discount_rate", "order_item_profit_ratio",
    "gross_sales", "discount_amount", "net_revenue",
    "order_profit_amount", "total_cost", "actual_unit_cost",
    "is_profit_bleeder", "markup_pct", "margin_leakage_pct",
    "price_segment", "trade_route",
    "state_order_count", "state_density_class"
]

def main():
    logging.info("Starting Silver → SQL fact load pipeline.")

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

    silver_path_no_scheme = SILVER_DIR.replace("s3://", "")
    parquet_files = fs.glob(f"{silver_path_no_scheme}/Fact_*.parquet")

    single_file = f"{silver_path_no_scheme}/DataCo_Silver.parquet"
    if fs.exists(single_file) and single_file not in parquet_files:
        parquet_files.append(single_file)

    if not parquet_files:
        logging.info("No fact Parquet files found to load.")
        return

    logging.info(f"Found {len(parquet_files)} files to load.")

    for i, file_path in enumerate(parquet_files, start=1):
        file_name = os.path.basename(file_path)
        full_s3_path = f"s3://{file_path}"
        logging.info(f"Processing file {i}/{len(parquet_files)}: {file_name}")

        try:
            with fs.open(full_s3_path, 'rb') as f:
                df = pl.read_parquet(f)

            available_columns = [col for col in STRICT_COLUMNS if col in df.columns]
            if len(available_columns) < len(STRICT_COLUMNS):
                missing = set(STRICT_COLUMNS) - set(available_columns)
                logging.warning(f"File missing strict columns: {missing}")

            df_clean = df.select(available_columns)
            logging.info(f"Loading {df_clean.height} rows into SQL.")

            df_clean.to_pandas().to_sql(
                name=TABLE_NAME,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            logging.info("Load successful.")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"LOADED_{file_name}_{timestamp}.parquet"
            archive_path = f"{ARCHIVE_DIR}/{archive_name}"

            fs.copy(file_path, archive_path.replace("s3://", ""))
            fs.rm(file_path)
            logging.info(f"Archived file as: {archive_path}")

        except Exception as e:
            logging.error(f"Error loading {file_name}: {e}", exc_info=True)
            logging.info("Skipping file.")

    logging.info("Silver → SQL pipeline completed.")

if __name__ == "__main__":
    main()