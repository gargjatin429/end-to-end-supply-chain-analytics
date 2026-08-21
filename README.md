# Supply Chain Failure Analysis: Revenue Growth Without Profit

> **An end-to-end analytical data engineering and BI case study investigating how operational inefficiencies, logistics failures, and fraud risk caused profit collapse despite strong revenue growth.**

![Status](https://img.shields.io/badge/Status-Complete-success)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20%7C%20MinIO%20S3-blue)
![Stack](https://img.shields.io/badge/Stack-Python%20%7C%20SQL%20Server%20%7C%20Airflow-yellow)

---

## 📌 Project Scope

Diagnose why strong revenue growth failed to translate into profit by identifying operational inefficiencies, logistics failures, and fraud risk across the supply chain.

### Technical & Architectural Focus
This project showcases a **modern, enterprise-grade data engineering workflow running entirely locally via Docker Compose**. It implements a strict **Medallion Architecture (Bronze → Silver → SQL)** powered by a local **MinIO S3 Object Store** and orchestrated by **Apache Airflow**.

- **Idempotent pipelines** stream data from S3 using `s3fs` and Polars `LazyFrames` for optimal memory management.
- **Microsoft SQL Server 2022** acts as the serving layer for dimensional star-schema modeling, loaded via atomic transactions.
- **SDV (CTGAN)** on Google Colab was used to synthesize 2 million rows of transaction data (using mathematically sound `FixedCombinations` constraints) to stress-test the SQL logic beyond standard Excel-scale limits.

---

## 🚀 How to Run the End-to-End Cluster

We have provided a fully containerized, enterprise-grade orchestration environment that spins up MinIO, Microsoft SQL Server, Postgres, Redis, and Apache Airflow with a single command.

### 1. Bootstrap Your Host Machine
Before running the cluster, ensure your local machine has the necessary dependencies. We provide a bootstrap script that checks for Docker, sets up a Python virtual environment, and installs testing libraries.

Run the following in your terminal:
```bash
chmod +x bootstrap_host.sh
./bootstrap_host.sh
```

### 2. Start the Cluster
Activate your virtual environment and start the Docker Compose cluster:
```bash
source .venv/bin/activate
docker-compose up -d --build
```

**What happens when you run this?**
1. **MinIO (`http://localhost:9001`)**: Spins up an S3-compatible data lake and creates the `bronze`, `silver`, and `archive` buckets.
2. **MS SQL Server (`localhost:1433`)**: Spins up an enterprise database and creates the `DataCo_Analytics` database.
3. **Apache Airflow (`http://localhost:8080`)**: Boots up the web UI, scheduler, and worker nodes.

*(Note: The cluster takes about 30-60 seconds to become fully healthy).*

### 3. Execute the Airflow Pipeline
1. Navigate to **http://localhost:8080** in your web browser.
2. Log in with:
   - **Username:** `airflow`
   - **Password:** `airflow`
3. In the DAGs list, find `supply_chain_medallion_pipeline`.
4. Click the "Play" button (▶) to **Trigger DAG**.

**The Pipeline Execution Flow:**
1. **`bootstrap_dimensions`**: Injects necessary lookup dimension tables into the Silver S3 bucket.
2. **`prep_raw_data`**: Executes our Polars CLI tool against the raw dataset (`DataCoSupplyChainDataset.csv`). It performs Unicode normalization, schema validation, and saves `model_ready.csv`.
3. **`simulate_sdv_upload`**: Simulates the external Google Colab ML generation step by uploading the clean CSV directly to the `bronze` MinIO bucket.
4. **`transform_bronze_to_silver`**: Triggers the Polars `LazyFrame` transformation script. Parses financials, joins dimensions, and writes highly-curated columnar Parquet files into the Silver S3 bucket.
5. **`load_silver_to_sql`**: Initiates an atomic database transaction using Polars `write_database` to bulk insert the analytical facts into the Microsoft SQL Server.

To tear down the cluster and wipe the data, run:
```bash
docker-compose down -v
```

---

## 📂 Repository Structure & Documentation

We highly recommend reviewing our detailed architectural documentation to understand how this pipeline evolved into its current enterprise-grade state:

- **[ARCHITECTURAL_METAMORPHOSIS.md](ARCHITECTURAL_METAMORPHOSIS.md)**: A massive, detailed breakdown comparing the "Before" and "After" code blocks of our pipeline, explaining the shift to LazyFrames, atomic transactions, and SDV constraints.
- **[refactored_workflow.md](refactored_workflow.md)**: A senior-engineer review of the Medallion pipeline logic.
- **[pre_bronze_review.md](pre_bronze_review.md)**: A detailed critique of the data scaling and CSV cleaning processes.

```text
├── docker-compose.yml    # Full cluster architecture
├── Dockerfile            # Custom Airflow image with Polars & ODBC drivers
├── bootstrap_host.sh     # Auto-installer for local execution
├── config.py             # Central S3/SQL configs with strict TEST_MODE security
├── dags/                 # Airflow Directed Acyclic Graphs
├── data_scaling/         # Polars CLI data prep and parameterized Colab ML notebook
├── pipelines/            # Bronze → Silver (Polars) and Silver → SQL (S3fs) scripts
├── scripts/              # Dimension bootstrapping
├── tests/                # Comprehensive pytest suite covering math edge cases
├── powerbi/              # The .pbix dashboard file and DAX measures
└── sql/                  # DDLs and analytical queries for SQL Server
```
