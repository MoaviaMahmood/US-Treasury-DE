# US Treasury Data Engineering Platform
Medallion Architecture | Spark | Airflow | PostgreSQL | Docker

---

## Project Overview

This project implements a production-style, end-to-end data engineering platform built on the Medallion Architecture pattern (Bronze → Silver → Gold).

The pipeline ingests official U.S. Treasury Fiscal Data APIs, processes data using Apache Spark, orchestrates workflows via Apache Airflow, and loads curated business-level datasets into PostgreSQL for analytics consumption.

The system is fully containerized using Docker Compose to simulate a scalable, modern data platform.

---
## Business Objective

Design and implement a modular, distributed data pipeline that:

- Ingests structured financial data from public APIs
- Preserves raw data for auditability
- Applies deterministic transformations and cleansing
- Produces analytics-ready aggregated datasets
- Loads curated data into a relational warehouse
- Enables workflow orchestration and observability

## Architecture

```
                ┌──────────────────┐
                │  Treasury APIs   │
                └─────────┬────────┘
                          ↓
                ┌──────────────────┐
                │  Bronze Layer    │  (Raw Parquet)
                └─────────┬────────┘
                          ↓
                ┌──────────────────┐
                │  Silver Layer    │  (Cleaned & Standardized)
                └─────────┬────────┘
                          ↓
                ┌──────────────────┐
                │   Gold Layer     │  (Business Aggregations)
                └─────────┬────────┘
                          ↓
                ┌──────────────────┐
                │  PostgreSQL DWH  │
                └─────────┬────────┘
                          ↓
                ┌──────────────────┐
                │     Airflow      │
                │   Orchestration  │
                └──────────────────┘


```
## Core Components

| Component           | Responsibility                  |
| ------------------- | ------------------------------- |
| **Apache Spark**    | Distributed data transformation |
| **Apache Airflow**  | Workflow orchestration          |
| **PostgreSQL**      | Analytics data warehouse        |
| **Docker Compose**  | Container orchestration         |
| **Parquet Storage** | Columnar data lake storage      |

## Data Sources

Data is extracted from the official U.S. Treasury Fiscal Data API:

- operating_cash_balance
- public_debt_transactions

Source:
https://api.fiscaldata.treasury.gov/

---
## Medallion Architecture Implementation

### Bronze Layer — Raw Ingestion

**Objective**: Preserve source data exactly as received.

- API responses stored in Parquet format
- Immutable append-only design
- Schema-on-read
- Ingestion metadata:
  - ingestion_timestamp
  - source_system

This layer ensures auditability and reproducibility.

### Silver Layer — Cleansed & Standardized

**Objective**: Produce reliable, structured datasets.

- Data type normalization
- Null handling and validation
- Helps enforce numeric consistency
- Deduplication
- Date standardization

This layer ensures transformation logic is deterministic and reusable.

### Gold Layer — Business Aggregations

**Objective**: Deliver analytics-ready datasets.

Generated outputs include:

- Daily totals
- Monthly totals
- Monthly debt growth
- Security-level summaries

These tables are written to PostgreSQL for BI consumption.

## Project Structure

```
FINANCE/
├── dags/
│   ├── treasury_dag.py
|   └── madelion_pipeline.py
|
├── data/
│   ├── bronze/                   # Raw ingested data (Parquet)
│   ├── silver/                   # Cleaned transformed data (Parquet)
│   └── gold/                     # Aggregated data (Parquet)
|
├── etl/
│   ├── __init__.py 
│   ├── extract.py               
│   ├── transform.py             
│   ├── load.py                  
│   └── pipeline.py
|
├── raw_data/                    # Raw CSV data from API
|
├── spark_jobs/
│   ├── bronze_ingest.py         # Spark job for bronze layer
│   ├── silver_transform.py      # Spark job for silver layer
│   ├── gold_aggregations.py     # Spark job for gold layer
|   └── load_spark.py            # Spark job for loading Gold layer to Docker Postgres
|
├── main.py                      # Main pipeline orchestration
├── docker-compose.yml           # Docker services configuration
├── requirements.txt             # Python dependencies
└── readme.md                    # Project documentation
```

## Running the Project

###  Start Services

```bash
docker compose up --build -d
```
Check running containers:
```bash
docker ps
```
Services:
- Airflow UI → http://localhost:8080
- Spark UI → http://localhost:8081
- PostgreSQL → localhost:5432

---
### Run Bronze → Silver → Gold in Spark
```bash
docker exec -it spark bash
```
Then:
```bash
/opt/spark/bin/spark-submit /opt/spark-apps/spark_jobs/gold_aggregations.py
```
Gold data will be written to:
```bash
/data/gold/
```

### Load Gold Layer to PostgreSQL

Ensure PostgreSQL JDBC driver exists:
```bash
ls /opt/spark/jars | grep postgresql
```

Run:
```bash
/opt/spark/bin/spark-submit \
--jars /opt/spark/jars/postgresql-42.7.3.jar \
/opt/spark-apps/spark_jobs/gold_to_postgres.py
```
### Validate Warehouse Tables
```bash
docker exec -it treasury_postgres psql -U postgres -d US_Treasury
```

Inside psql:
```bash
\dt
SELECT * FROM gold_monthly_totals LIMIT 5;
```

Expected tables:
```bash
gold_daily_totals
gold_monthly_totals
gold_monthly_debt_growth
gold_security_totals
```
### Airflow Orchestration

Airflow DAG orchestrates:

- Bronze ingestion
- Silver transformation
- Gold aggregation
- Gold load to PostgreSQL

Access Airflow UI:
```bash
http://localhost:8080
```
---

## Engineering Design Principles

- Separation of storage, compute, and orchestration
- Immutable raw data layer
- Deterministic transformation logic
- Reproducible containerized environment
- Modular Spark job design
- Clear layer responsibility boundaries

## Technical Capabilities Demonstrated

- Distributed processing with PySpark
- JDBC-based warehouse loading
- Workflow orchestration with Airflow
- Medallion data lake architecture
- Containerized data platform setup
- Structured data modeling for analytics

## Roadmap / Enhancements

- Incremental loading strategy
- Table partitioning in Gold layer
- PostgreSQL indexing & performance tuning
- Star schema dimensional modeling
- Data quality validation layer
- CI/CD for DAG deployment
- Cloud-native deployment (AWS / GCP)

## Author

**Moavia Mahmood**  
Data Engineer

Focused on building scalable data platforms, distributed pipelines, and production-grade ETL systems.