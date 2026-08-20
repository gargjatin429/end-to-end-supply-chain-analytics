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

    # PHASE 1: DISCOVERY
    # Remove the s3:// prefix for s3fs globbing
    bronze_path_no_scheme = BRONZE_DIR.replace("s3://", "")
    csv_files = fs.glob(f"{bronze_path_no_scheme}/*.csv")

    logging.info(f"Found {len(csv_files)} files to process in {BRONZE_DIR}.")

    # PHASE 2: BATCH PROCESSING
    for i, file_path in enumerate(csv_files, start=1):
        file_name = os.path.basename(file_path)
        full_s3_path = f"s3://{file_path}"
        logging.info(f"Processing file {i}/{len(csv_files)}: {file_name}")

        try:
            # STEP 1: LOAD
            with fs.open(full_s3_path, 'rb') as f:
                df = pl.read_csv(f, encoding="cp1252")

            # STEP 2-7: TRANSFORMATIONS
            df_silver = transform_bronze_to_silver(df)

            # STEP 7: WRITE
            output_name = f"Fact_{os.path.splitext(file_name)[0]}.parquet"
            output_path = f"{SILVER_DIR}/{output_name}"

            with fs.open(output_path, 'wb') as f:
                df_silver.write_parquet(f)

            logging.info(f"Saved cleaned data: {output_path}")

            # STEP 8: ARCHIVAL
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.csv"
            archive_path = f"{ARCHIVE_DIR}/{archive_name}"

            # Move file in S3
            fs.copy(file_path, archive_path.replace("s3://", ""))
            fs.rm(file_path)
            logging.info(f"Archived source file: {archive_path}")

        except Exception as e:
            logging.error(f"Error processing {file_name}: {e}", exc_info=True)
            logging.info("Skipping file and continuing batch job.")

    logging.info("Batch processing complete.")

if __name__ == "__main__":
    main()