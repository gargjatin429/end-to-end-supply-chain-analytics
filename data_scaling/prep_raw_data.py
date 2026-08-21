import polars as pl
import argparse
import logging
import unicodedata
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_text_series(series: pl.Series) -> pl.Series:
    """Normalize accented or special Unicode characters to plain ASCII."""
    # We apply this element-wise since unicodedata doesn't have a direct Polars native binding
    return series.map_elements(
        lambda x: ''.join(c for c in unicodedata.normalize('NFD', str(x)) if unicodedata.category(c) != 'Mn') if x is not None else "",
        return_dtype=pl.String
    )

def standardize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Standardize column names to snake_case."""
    new_cols = {}
    for col in df.columns:
        clean_name = col.strip().lower()
        clean_name = clean_name.replace(" ", "_").replace("(", "_").replace(")", "_")
        while "__" in clean_name:
            clean_name = clean_name.replace("__", "_")
        clean_name = clean_name.strip("_")
        new_cols[col] = clean_name
    return df.rename(new_cols)

def main():
    parser = argparse.ArgumentParser(description="Clean raw supply chain dataset for SDV training.")
    parser.add_argument("--input", "-i", required=True, help="Path to the raw CSV file")
    parser.add_argument("--output", "-o", required=True, help="Path to save the cleaned model-ready CSV")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        logging.error(f"Input file not found: {args.input}")
        return

    logging.info(f"Loading raw dataset from {args.input} (using cp1252 to handle broken latin-1 encoding)")

    try:
        df = pl.read_csv(args.input, encoding="cp1252", ignore_errors=True)
    except Exception as e:
        logging.error(f"Failed to read CSV: {e}")
        return

    # 1. Standardize Columns first to make downstream references easy
    df = standardize_columns(df)

    # 2. Identify text columns and normalize Unicode
    logging.info("Normalizing text columns...")
    text_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.String]
    df = df.with_columns([
        normalize_text_series(df[col]).alias(col) for col in text_cols
    ])

    # 3. Date Repair Logic
    logging.info("Parsing and repairing dates...")
    if "order_date_dateorders" in df.columns:
        df = df.with_columns(
            pl.col("order_date_dateorders").str.replace_all("/", "-").str.replace_all("  ", " ").str.strip_chars().alias("date_str_clean")
        ).with_columns(
            parsed_date=pl.coalesce(
                pl.col("date_str_clean").str.to_datetime("%m-%d-%Y %H:%M", strict=False),
                pl.col("date_str_clean").str.to_datetime("%m-%d-%Y %I:%M:%S %p", strict=False),
                pl.col("date_str_clean").str.to_datetime("%m-%d-%y %H:%M", strict=False)
            )
        ).with_columns(
            order_year=pl.col("parsed_date").dt.year(),
            order_month=pl.col("parsed_date").dt.month(),
            order_day=pl.col("parsed_date").dt.day()
        )

        # Fix ancient years and flag invalid ones explicitly
        df = df.with_columns(
            order_year=pl.when(pl.col("order_year") < 1900).then(pl.col("order_year") + 2000).otherwise(pl.col("order_year"))
        )

        # Safe bounds check
        invalid_years = df.filter((pl.col("order_year") < 2010) | (pl.col("order_year") > 2025)).height
        if invalid_years > 0:
            logging.warning(f"Found {invalid_years} rows with years outside expected bounds (2010-2025). They will be dropped.")
            df = df.filter((pl.col("order_year") >= 2010) & (pl.col("order_year") <= 2025))

        df = df.with_columns(
            order_dayofweek=pl.date(pl.col("order_year"), pl.col("order_month"), pl.col("order_day")).dt.weekday()
        ).drop(["date_str_clean", "parsed_date"])

    # 4. Drop Unused Columns
    logging.info("Pruning unused/sensitive columns...")
    cols_to_drop = [
        'customer_email', 'customer_fname', 'customer_lname', 'customer_password',
        'customer_street', 'customer_zipcode', 'order_id', 'order_item_id',
        'customer_id', 'order_customer_id', 'product_card_id',
        'order_item_cardprod_id', 'category_id', 'department_id',
        'product_category_id', 'product_description', 'product_image',
        'latitude', 'longitude', 'benefit_per_order', 'sales_per_customer',
        'delivery_status', 'late_delivery_risk', 'customer_city', 'order_city',
        'order_item_discount', 'sales', 'order_item_total', 'order_profit_per_order',
        'order_zipcode', 'product_price', 'product_status',
        'shipping_date_dateorders'
    ]
    existing_cols = [c for c in cols_to_drop if c in df.columns]
    df = df.drop(existing_cols)

    # 5. Export
    logging.info(f"Saving {df.height} rows to {args.output}")
    df.write_csv(args.output)
    logging.info("Data preparation complete.")

if __name__ == "__main__":
    main()