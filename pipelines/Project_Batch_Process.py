import polars as pl
import s3fs
import os
import logging
from datetime import datetime
import sys
from urllib.parse import urlparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BRONZE_DIR, SILVER_DIR, ARCHIVE_DIR, DIM_GEO_PATH, DIM_CUST_PATH, DIM_PROD_PATH, get_s3_storage_options, S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_KEY
from pipelines.transformations import transform_bronze_to_silver, DataValidationError, IncompleteDimensionError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_s3_path(uri: str) -> str:
    """Safely extract the path component from an S3 URI."""
    parsed = urlparse(uri)
    return f"{parsed.netloc}{parsed.path}"

def main():
    try:
        fs = s3fs.S3FileSystem(
            key=S3_ACCESS_KEY,
            secret=S3_SECRET_KEY,
            client_kwargs={'endpoint_url': S3_ENDPOINT_URL}
        )
    except Exception as e:
        logging.critical(f"Failed to initialize S3 File System: {e}")
        sys.exit(1)

    # PHASE 0: PRE-LOAD DIMENSIONS
    storage_options = get_s3_storage_options()
    logging.info("Pre-loading dimensions into memory.")
    try:
        dim_geo = pl.read_parquet(DIM_GEO_PATH, storage_options=storage_options)
        dim_cust = pl.read_parquet(DIM_CUST_PATH, storage_options=storage_options)
        dim_prod = pl.read_parquet(DIM_PROD_PATH, storage_options=storage_options)
    except Exception as e:
        logging.critical(f"Failed to load dimension tables from S3: {e}")
        sys.exit(1)

    # PHASE 1: DISCOVERY
    bronze_path = get_s3_path(BRONZE_DIR)
    csv_files = fs.glob(f"{bronze_path}/*.csv")
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
            df_silver = transform_bronze_to_silver(df, dim_geo, dim_cust, dim_prod)

            # STEP 7: WRITE
            output_name = f"Fact_{os.path.splitext(file_name)[0]}.parquet"
            output_path = f"{SILVER_DIR}/{output_name}"

            with fs.open(get_s3_path(output_path), 'wb') as f:
                df_silver.write_parquet(f)

            logging.info(f"Saved cleaned data: {output_path}")

            # STEP 8: ARCHIVAL
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.csv"
            archive_path = f"{ARCHIVE_DIR}/{archive_name}"

            # Safely move file
            fs.copy(file_path, get_s3_path(archive_path))
            fs.rm(file_path)
            logging.info(f"Archived source file: {archive_path}")

        except (DataValidationError, IncompleteDimensionError, pl.ComputeError, pl.SchemaError) as e:
            logging.error(f"Data error in {file_name}: {e}. Skipping file.")
        except Exception as e:
            # Catch-all for S3 or systemic errors
            logging.critical(f"System failure processing {file_name}: {e}", exc_info=True)
            logging.info("Aborting batch job to prevent partial state corruption.")
            sys.exit(1)

    logging.info("Batch processing complete.")

if __name__ == "__main__":
    main()