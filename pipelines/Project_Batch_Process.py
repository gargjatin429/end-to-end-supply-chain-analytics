import polars as pl
import shutil
import os
import glob
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

    # ==============================================================================
    # PHASE 1: DISCOVERY
    # ==============================================================================
    csv_files = glob.glob(os.path.join(BRONZE_DIR, "*.csv"))
    logging.info(f"Found {len(csv_files)} files to process in {BRONZE_DIR}.")

    # ==============================================================================
    # PHASE 2: BATCH PROCESSING
    # ==============================================================================
    for i, file_path in enumerate(csv_files, start=1):
        file_name = os.path.basename(file_path)
        logging.info(f"Processing file {i}/{len(csv_files)}: {file_name}")

        try:
            # STEP 1: LOAD (Extract)
            df = pl.read_csv(file_path, encoding="cp1252")

            # STEP 2-7: TRANSFORMATIONS
            df_silver = transform_bronze_to_silver(df)

            # STEP 7: WRITE
            output_name = f"Fact_{os.path.splitext(file_name)[0]}.parquet"
            output_path = os.path.join(SILVER_DIR, output_name)
            df_silver.write_parquet(output_path)
            logging.info(f"Saved cleaned data: {output_path}")

            # STEP 8: ARCHIVAL (IDEMPOTENCY)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.csv"
            archive_path = os.path.join(ARCHIVE_DIR, archive_name)
            shutil.move(file_path, archive_path)
            logging.info(f"Archived source file: {archive_path}")

        except Exception as e:
            logging.error(f"Error processing {file_name}: {e}", exc_info=True)
            logging.info("Skipping file and continuing batch job.")

    logging.info("Batch processing complete.")

if __name__ == "__main__":
    main()
