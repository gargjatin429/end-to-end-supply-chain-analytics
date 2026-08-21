# End-to-End Local Orchestration Guide

This project features a fully containerized, enterprise-grade data engineering orchestration environment powered by **Apache Airflow**, **MinIO (S3)**, and **Microsoft SQL Server**.

It is designed to run seamlessly on a modern local machine (e.g., Ryzen 7 5700G, 16GB RAM) using Docker Compose.

## Prerequisites
- **Docker Desktop** or Docker Engine
- **Docker Compose** (usually bundled with Docker Desktop)

## 1. Quick Start

Open your terminal in the root of this repository and run the following command to build the custom Airflow image (which installs Polars and SQL Server drivers) and spin up the entire cluster in the background:

```bash
docker-compose up -d --build
```

### What happens when you run this?
1. **MinIO (`http://localhost:9001`)**: Spins up an S3-compatible data lake and creates the `bronze`, `silver`, and `archive` buckets.
2. **MS SQL Server (`localhost:1433`)**: Spins up an enterprise database and runs a script to create the `DataCo_Analytics` database.
3. **PostgreSQL & Redis**: Background services to support Airflow's distributed CeleryExecutor.
4. **Apache Airflow (`http://localhost:8080`)**: Boots up the web UI, scheduler, and worker node.

*Note: The cluster takes about 30-60 seconds to become fully healthy as databases initialize.*

## 2. Running the End-to-End Test Pipeline

We have provided an Airflow Directed Acyclic Graph (DAG) named `supply_chain_medallion_pipeline` that tests the entire architecture from start to finish.

1. Navigate to **http://localhost:8080** in your web browser.
2. Log in with:
   - **Username:** `airflow`
   - **Password:** `airflow`
3. In the DAGs list, find `supply_chain_medallion_pipeline`.
4. Click the "Play" button (▶) under the Actions column and select **Trigger DAG**.

### The Pipeline Execution Flow
Click on the DAG name and go to the **Graph** view to watch it execute in real-time. It performs four distinct tasks:

1. **`prep_raw_data`**: Executes our robust Polars Python CLI tool against the raw dataset (`DataCoSupplyChainDataset.csv`). It performs aggressive Unicode normalization, date bounding, schema validation, and saves the result as `model_ready.csv`.
2. **`simulate_sdv_upload_to_bronze`**: Because training the SDV CTGAN model takes significant time on Google Colab, this task simulates that external step. It takes the cleaned CSV and uploads it directly to the local MinIO `bronze` bucket.
3. **`transform_bronze_to_silver`**: Triggers the Polars transformation script. It reads from Bronze S3, parses financials and dates, drops invalid dimensions, and writes highly-curated columnar Parquet files into the Silver S3 bucket (while archiving the Bronze CSV).
4. **`load_silver_to_sql`**: The final step. It pulls the Silver Parquet data and initiates an atomic database transaction using Polars `write_database` to bulk insert the analytical facts into the running Microsoft SQL Server instance.

## 3. Teardown

To shut down the cluster and wipe the data (including the databases and S3 buckets), run:

```bash
docker-compose down -v
```
