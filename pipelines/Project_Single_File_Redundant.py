import polars as pl
import s3fs
import os
import logging
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BRONZE_DIR, SILVER_DIR, ARCHIVE_DIR, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY
from pipelines.transformations import transform_bronze_to_silver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    fs = s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
    )

    source_file_name = "DataCo_Final_2M.csv"
    source_file_path = f"{BRONZE_DIR}/{source_file_name}"

    target_file_name = "DataCo_Silver.parquet"
    target_file_path = f"{SILVER_DIR}/{target_file_name}"

    if not fs.exists(source_file_path.replace("s3://", "")):
        logging.error(f"Source file not found: {source_file_path}")
        return

    logging.info(f"Starting single-file pipeline for: {source_file_name}")

    try:
        # STEP 1: LOAD
        with fs.open(source_file_path, 'rb') as f:
            df = pl.read_csv(f, encoding="cp1252")

        logging.info(f"Original row count: {df.height}")

        # STEP 2-7: TRANSFORMATIONS
        df_silver = transform_bronze_to_silver(df)

        # STEP 7: WRITE
        with fs.open(target_file_path, 'wb') as f:
            df_silver.write_parquet(f)

        logging.info(f"Processed file saved to: {target_file_path}")
        logging.info(f"Final row count: {df_silver.height}")

        # STEP 8: ARCHIVAL
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(source_file_name)
        archive_name = f"{name}_{timestamp}{ext}"
        archive_path = f"{ARCHIVE_DIR}/{archive_name}"

        # Move file in S3
        fs.copy(source_file_path.replace("s3://", ""), archive_path.replace("s3://", ""))
        fs.rm(source_file_path.replace("s3://", ""))

        logging.info(f"Archived source file as: {archive_path}")

    except Exception as e:
        logging.error(f"Pipeline failed. Error details: {e}", exc_info=True)

if __name__ == "__main__":
    main()