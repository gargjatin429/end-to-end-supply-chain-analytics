import polars as pl
import shutil
import os
import glob
import logging
from sqlalchemy import create_engine
from datetime import datetime
import sys

# Add the project root to the python path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import SILVER_DIR, ARCHIVE_DIR, SQL_SERVER_NAME, SQL_DATABASE, SQL_DRIVER, ensure_directories

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TABLE_NAME = "Fact_Sales"

# Explicit column contract matching the SQL table schema exactly
STRICT_COLUMNS = [
    # Keys
    "geo_id", "customer_geo_id", "product_key",

    # Time (year / month / day only)
    "order_year", "order_month", "order_day",
    "day_name_str", "order_day_type",

    # Logistics
    "type", "days_for_shipping_real", "days_for_shipment_scheduled",
    "shipping_delta", "delivery_class", "shipping_mode_clean",
    "order_status", "customer_segment",

    # Financials
    "order_item_quantity", "order_item_product_price",
    "order_item_discount_rate", "order_item_profit_ratio",
    "gross_sales", "discount_amount", "net_revenue",
    "order_profit_amount", "total_cost", "actual_unit_cost",

    # Metrics
    "is_profit_bleeder", "markup_pct", "margin_leakage_pct",
    "price_segment", "trade_route",
    "state_order_count", "state_density_class"
]

def main():
    ensure_directories()
    logging.info("Starting Silver → SQL fact load pipeline.")

    # --------------------------------------------------------------------------
    # STEP 1: CONNECT TO SQL SERVER (OR SQLITE FOR TESTING)
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
        # fast_executemany=True is highly recommended for pyodbc bulk inserts
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
    # STEP 2: DISCOVER FACT FILES
    # --------------------------------------------------------------------------
    parquet_files = glob.glob(os.path.join(SILVER_DIR, "Fact_*.parquet"))

    # Also include the single file output if it exists
    single_file = os.path.join(SILVER_DIR, "DataCo_Silver.parquet")
    if os.path.exists(single_file) and single_file not in parquet_files:
        parquet_files.append(single_file)

    if not parquet_files:
        logging.info("No fact Parquet files found to load.")
        return

    logging.info(f"Found {len(parquet_files)} files to load.")

    # --------------------------------------------------------------------------
    # STEP 3: LOAD LOOP
    # --------------------------------------------------------------------------
    for i, file_path in enumerate(parquet_files, start=1):
        file_name = os.path.basename(file_path)
        logging.info(f"Processing file {i}/{len(parquet_files)}: {file_name}")

        try:
            # Read Parquet
            df = pl.read_parquet(file_path)

            # Enforce strict schema alignment
            # Using list comprehension to get only columns that exist in df, to avoid errors if testing schema differs slightly
            available_columns = [col for col in STRICT_COLUMNS if col in df.columns]
            if len(available_columns) < len(STRICT_COLUMNS):
                missing = set(STRICT_COLUMNS) - set(available_columns)
                logging.warning(f"File missing strict columns: {missing}")

            df_clean = df.select(available_columns)
            logging.info(f"Loading {df_clean.height} rows into SQL.")

            # Append to SQL table. We still use to_pandas() for sqlalchemy compatibility,
            # but fast_executemany=True on engine setup makes it orders of magnitude faster.
            df_clean.to_pandas().to_sql(
                name=TABLE_NAME,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            logging.info("Load successful.")

            # Archive processed file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"LOADED_{file_name}_{timestamp}.parquet"
            archive_path = os.path.join(ARCHIVE_DIR, archive_name)

            shutil.move(file_path, archive_path)
            logging.info(f"Archived file as: {archive_name}")

        except Exception as e:
            logging.error(f"Error loading {file_name}: {e}", exc_info=True)
            logging.info("Skipping file.")

    logging.info("Silver → SQL pipeline completed.")

if __name__ == "__main__":
    main()
