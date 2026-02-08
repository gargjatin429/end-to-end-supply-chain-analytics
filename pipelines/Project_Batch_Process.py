"""
Purpose:
    Batch Bronze → Silver processing pipeline for supply chain data.

What this script does:
    - Reads raw CSV files from the Bronze layer
    - Cleans and validates records (dates, duplicates, schema issues)
    - Derives financial, operational, and strategic analytical fields
    - Joins curated dimension tables to form a star-schema-ready fact dataset
    - Writes cleaned Parquet files to the Silver layer
    - Archives processed source files to ensure idempotent re-runs

What this script does NOT do:
    - No model training or synthetic data generation
    - No SQL loading or BI logic
    - No production orchestration or scheduling
"""

import polars as pl
import shutil
import os
import glob
from datetime import datetime

# ==============================================================================
# CONFIGURATION & PATHS
# ==============================================================================
# Data Lake Zones
bronze_folder_path = r"D:\Data Lake\Bronze"
silver_folder_path = r"D:\Data Lake\Silver"
archive_folder_path = r"D:\Data Lake\Archive"

# Dimension Tables (pre-cleaned, static Parquet files)
dim_geo_path = r"D:\Data Lake\Silver\dim_geo.parquet"
dim_cust_path = r"D:\Data Lake\Silver\Dim_Customer_Geo.parquet"
dim_prod_path = r"D:\Data Lake\Silver\Dim_Product.parquet"

# ==============================================================================
# PHASE 1: DISCOVERY
# ==============================================================================
# Identify all CSV files present in the Bronze layer
csv_files = glob.glob(os.path.join(bronze_folder_path, "*.csv"))
print(f"Found {len(csv_files)} files to process.\n")

# ==============================================================================
# PHASE 2: BATCH PROCESSING
# ==============================================================================
for i, file_path in enumerate(csv_files, start=1):

    file_name = os.path.basename(file_path)
    print(f"Processing file {i}/{len(csv_files)}: {file_name}")

    try:
        # ----------------------------------------------------------------------
        # STEP 1: LOAD (Extract)
        # ----------------------------------------------------------------------
        df = pl.read_csv(file_path, encoding="cp1252")

        # ----------------------------------------------------------------------
        # STEP 2: DATA VALIDATION & CLEANUP
        # ----------------------------------------------------------------------
        # Validate dates first to avoid propagating invalid records downstream
        df = (
            df
            .with_columns(
                pl.format(
                    "{}-{}-{}",
                    pl.col("order_year"),
                    pl.col("order_month"),
                    pl.col("order_day")
                )
                .str.to_date("%Y-%m-%d", strict=False)
                .alias("valid_date_check")
            )
            .filter(pl.col("valid_date_check").is_not_null())
        )

        # Deduplication while preserving source order
        rows_before = df.height
        df = df.unique(maintain_order=True)
        rows_after = df.height

        if rows_before != rows_after:
            print(f"  Dropped {rows_before - rows_after} duplicate rows.")

        # Remove helper and unused source columns
        df = df.drop([
            "order_dayofweek",
            "valid_date_check",
            "shipping_mode"
        ])

        # ----------------------------------------------------------------------
        # STEP 3: FINANCIAL METRIC DERIVATION
        # ----------------------------------------------------------------------
        df = (
            df
            .with_columns([
                (pl.col("order_item_product_price") * pl.col("order_item_quantity"))
                .alias("gross_sales"),

                (
                    (pl.col("order_item_product_price") * pl.col("order_item_quantity"))
                    * pl.col("order_item_discount_rate")
                ).alias("discount_amount")
            ])
            .with_columns([
                (pl.col("gross_sales") - pl.col("discount_amount"))
                .alias("net_revenue")
            ])
            .with_columns([
                (pl.col("net_revenue") * pl.col("order_item_profit_ratio"))
                .alias("order_profit_amount")
            ])
            .with_columns([
                (pl.col("net_revenue") - pl.col("order_profit_amount"))
                .alias("total_cost")
            ])
        )

        # ----------------------------------------------------------------------
        # STEP 4: OPERATIONAL & STRATEGIC FEATURES
        # ----------------------------------------------------------------------
        df = (
            df
            .with_columns([
                (pl.col("total_cost") / pl.col("order_item_quantity"))
                .alias("actual_unit_cost"),

                (pl.col("order_profit_amount") < 0)
                .alias("is_profit_bleeder"),

                (pl.col("days_for_shipping_real")
                 - pl.col("days_for_shipment_scheduled"))
                .alias("shipping_delta")
            ])
            .with_columns([
                (
                    (pl.col("order_item_product_price") - pl.col("actual_unit_cost"))
                    / pl.col("actual_unit_cost")
                ).alias("markup_pct"),

                (
                    pl.col("discount_amount")
                    / (pl.col("order_profit_amount") + pl.col("discount_amount"))
                ).fill_nan(0.0).alias("margin_leakage_pct")
            ])
        )

        # Categorical segmentation for analysis
        df = df.with_columns([
            pl.when(pl.col("shipping_delta") < 0).then("Early")
              .when(pl.col("shipping_delta") == 0).then("On Time")
              .otherwise("Late")
              .alias("delivery_class"),

            pl.when(pl.col("days_for_shipment_scheduled") == 0).then("Same Day")
              .when(pl.col("days_for_shipment_scheduled") <= 2).then("First Class")
              .when(pl.col("days_for_shipment_scheduled") == 3).then("Second Class")
              .otherwise("Standard Class")
              .alias("shipping_mode_clean"),

            pl.date(
                pl.col("order_year"),
                pl.col("order_month"),
                pl.col("order_day")
            ).dt.strftime("%A").alias("day_name_str"),

            pl.when(
                pl.date(
                    pl.col("order_year"),
                    pl.col("order_month"),
                    pl.col("order_day")
                )
                .dt.strftime("%A")
                .is_in(["Saturday", "Sunday"])
            )
            .then("Weekend")
            .otherwise("Weekday")
            .alias("order_day_type"),

            pl.when(pl.col("order_item_product_price") < 60).then("Budget")
              .when(pl.col("order_item_product_price") <= 250).then("Mainstream")
              .otherwise("Premium")
              .alias("price_segment"),

            (
                pl.col("customer_country").str.replace("EE. UU.", "USA")
                + "_"
                + pl.col("customer_state")
                + " -> "
                + pl.col("order_country")
            ).alias("trade_route")
        ])

        # ----------------------------------------------------------------------
        # STEP 5: CONTEXTUAL WINDOW METRICS
        # ----------------------------------------------------------------------
        df = (
            df
            .with_columns([
                (pl.col("gross_sales")
                 / pl.col("gross_sales").sum().over("category_name"))
                .alias("category_share_pct"),

                pl.col("order_state").count().over("order_state")
                .alias("state_order_count"),

                (pl.col("gross_sales")
                 / pl.col("gross_sales").sum().over("market"))
                .alias("market_share_pct")
            ])
            .with_columns([
                pl.when(pl.col("state_order_count") > 100).then("Strategic Hub")
                  .when(pl.col("state_order_count") < 10).then("Expansion Zone")
                  .otherwise("Standard Zone")
                  .alias("state_density_class")
            ])
        )

        # ----------------------------------------------------------------------
        # STEP 6: STAR SCHEMA ENRICHMENT
        # ----------------------------------------------------------------------
        dim_geo = pl.read_parquet(dim_geo_path)
        dim_cust = pl.read_parquet(dim_cust_path)
        dim_prod = pl.read_parquet(dim_prod_path)

        df = (
            df
            .join(dim_geo,
                  on=["order_state", "order_country", "order_region", "market"],
                  how="left")
            .drop(["order_state", "order_country", "order_region", "market"])
            .join(dim_cust,
                  on=["customer_state", "customer_country"],
                  how="left")
            .drop(["customer_state", "customer_country"])
            .join(dim_prod,
                  on=["product_name", "category_name", "department_name"],
                  how="left")
            .drop(["product_name", "category_name", "department_name"])
        )

        # ----------------------------------------------------------------------
        # STEP 7: FINAL SORT & WRITE
        # ----------------------------------------------------------------------
        # Sorting ensures stable downstream clustered indexing in SQL
        df = df.sort(
            ["order_year", "order_month", "order_day", "order_item_quantity"]
        )

        # Normalize column naming
        df = df.rename({col: col.lower() for col in df.columns})

        output_name = f"Fact_{os.path.splitext(file_name)[0]}.parquet"
        df.write_parquet(os.path.join(silver_folder_path, output_name))
        print(f"  Saved cleaned data: {output_name}")

        # ----------------------------------------------------------------------
        # STEP 8: ARCHIVAL (IDEMPOTENCY)
        # ----------------------------------------------------------------------
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.csv"
        shutil.move(file_path, os.path.join(archive_folder_path, archive_name))

        print(f"  Archived source file: {archive_name}\n")

    except Exception as e:
        print(f"  Error processing {file_name}: {e}")
        print("  Skipping file and continuing batch job.\n")

print("Batch processing complete.")
