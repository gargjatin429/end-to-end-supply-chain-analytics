import polars as pl
import shutil
import os
import logging
from datetime import datetime
import sys

# Add the project root to the python path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BRONZE_DIR, SILVER_DIR, ARCHIVE_DIR, ensure_directories
from pipelines.transformations import transform_bronze_to_silver

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    ensure_directories()

    # Source file (Bronze Layer)
    source_file_name = "DataCo_Final_2M.csv"
    source_file_path = os.path.join(BRONZE_DIR, source_file_name)

    # Target output (Silver Layer)
    target_file_name = "DataCo_Silver.parquet"
    target_file_path = os.path.join(SILVER_DIR, target_file_name)

    if not os.path.exists(source_file_path):
        logging.error(f"Source file not found: {source_file_path}")
        return

    logging.info(f"Starting single-file pipeline for: {source_file_name}")

    try:
        # STEP 1: LOAD (Extract)
        df = pl.read_csv(source_file_path, encoding="cp1252")
        logging.info(f"Original row count: {df.height}")

        # STEP 2-7: TRANSFORMATIONS
        df_silver = transform_bronze_to_silver(df)

        # STEP 7: WRITE
        df_silver.write_parquet(target_file_path)
        logging.info(f"Processed file saved to: {target_file_path}")
        logging.info(f"Final row count: {df_silver.height}")

        # STEP 8: ARCHIVAL (IDEMPOTENCY)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(source_file_name)
        archive_name = f"{name}_{timestamp}{ext}"
        archive_path = os.path.join(ARCHIVE_DIR, archive_name)

        shutil.move(source_file_path, archive_path)
        logging.info(f"Archived source file as: {archive_path}")

    except Exception as e:
        logging.error(f"Pipeline failed. Error details: {e}", exc_info=True)

if __name__ == "__main__":
    main()
