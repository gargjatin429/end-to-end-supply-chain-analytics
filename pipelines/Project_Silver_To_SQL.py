"""
Purpose:
    Load curated Silver-layer fact data into SQL Server.

What this script does:
    - Reads fact-level Parquet files from the Silver layer
    - Enforces a strict column contract aligned with the SQL table schema
    - Appends data into the SQL Server fact table
    - Archives successfully loaded files to ensure idempotent execution

What this script does NOT do:
    - No transformations or business logic
    - No dimensional modeling
    - No table creation or schema changes
"""

import polars as pl
import pandas as pd
import shutil
import os
import glob
from sqlalchemy import create_engine
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SERVER_NAME = "localhost"
DATABASE = "DataCo_Analytics"
DRIVER = "ODBC Driver 17 for SQL Server"

SILVER_FOLDER = r"D:\Data Lake\Silver"
ARCHIVE_FOLDER = r"D:\Data Lake\archive_silver"
TABLE_NAME = "Fact_Sales"

connection_string = (
    f"mssql+pyodbc://@{SERVER_NAME}/{DATABASE}"
    f"?driver={DRIVER}&trusted_connection=yes"
)

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

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    print("Starting Silver → SQL fact load pipeline.")

    # --------------------------------------------------------------------------
    # STEP 1: CONNECT TO SQL SERVER
    # --------------------------------------------------------------------------
    try:
        engine = create_engine(connection_string)
        with engine.connect():
            pass
        print("Connected to SQL Server.")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # --------------------------------------------------------------------------
    # STEP 2: DISCOVER FACT FILES
    # --------------------------------------------------------------------------
    parquet_files = glob.glob(os.path.join(SILVER_FOLDER, "Fact_*.parquet"))

    if not parquet_files:
        print("No fact Parquet files found to load.")
        return

    print(f"Found {len(parquet_files)} files to load.\n")

    # --------------------------------------------------------------------------
    # STEP 3: LOAD LOOP
    # --------------------------------------------------------------------------
    for i, file_path in enumerate(parquet_files, start=1):
        file_name = os.path.basename(file_path)
        print(f"Processing file {i}/{len(parquet_files)}: {file_name}")

        try:
            # Read Parquet
            df = pl.read_parquet(file_path)

            # Enforce strict schema alignment
            df_clean = df.select(STRICT_COLUMNS)
            print(f"Loading {df_clean.height} rows into SQL.")

            # Append to SQL table
            df_clean.to_pandas().to_sql(
                name=TABLE_NAME,
                con=engine,
                if_exists="append",
                index=False,
                chunksize=10_000
            )

            print("Load successful.")

            # Archive processed file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"LOADED_{file_name}_{timestamp}.parquet"
            os.makedirs(ARCHIVE_FOLDER, exist_ok=True)
            shutil.move(file_path, os.path.join(ARCHIVE_FOLDER, archive_name))

            print(f"Archived file as: {archive_name}\n")

        except Exception as e:
            print(f"Error loading {file_name}: {e}")
            print("Skipping file.\n")

    print("Silver → SQL pipeline completed.")

if __name__ == "__main__":
    main()
