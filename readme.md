# 🇺🇸 US Treasury Data Engineering Pipeline  
Medallion Architecture (Bronze → Silver → Gold) using Airflow, Spark & PostgreSQL

---

## 📌 Project Overview

This project builds an end-to-end Data Engineering pipeline using official US Treasury Fiscal Data APIs.

It demonstrates:

- API-based data ingestion
- Medallion Architecture (Bronze / Silver / Gold layers)
- Distributed processing with Apache Spark
- Orchestration with Apache Airflow
- Data warehouse loading into PostgreSQL
- Fully containerized environment using Docker Compose

The goal is to simulate a production-grade modern data platform.

---

## 🏗 Architecture

```
Raw API Data → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → PostgreSQL
     ↑              ↑              ↑
   Airflow        Spark          Spark
```

### Components

- **Apache Airflow** → Orchestrates pipeline tasks
- **Apache Spark** → Performs distributed transformations
- **PostgreSQL** → Stores analytics-ready data
- **Docker Compose** → Container orchestration

---

## 📊 Data Sources

Data is extracted from:

- `operating_cash_balance`
- `public_debt_transactions`

Source:
https://api.fiscaldata.treasury.gov/

---

## 🥉 Bronze Layer (Raw)

- Stores raw API responses
- Schema-on-read
- Immutable append-only storage
- Stored in Parquet format
- Adds ingestion metadata:
  - ingestion_timestamp
  - source_system

Purpose:
Preserve original data exactly as received.

---

## 🥈 Silver Layer (Cleaned)

- Standardizes data types
- Handles null values
- Deduplicates records
- Converts date fields
- Ensures numeric consistency

Purpose:
Create reliable, analytics-ready structured data.

---

## 🥇 Gold Layer (Business Logic)

- Aggregated metrics
- Monthly debt trends
- Fiscal year summaries
- Transaction breakdowns

Purpose:
Deliver data ready for dashboards or BI tools.

---

## 📂 Project Structure

```
FINANCE/
├── dags/
│   └── treasury_dag.py           # Airflow DAG definition
├── data/
│   ├── bronze/                   # Raw ingested data (Parquet)
│   ├── silver/                   # Cleaned transformed data (Parquet)
│   └── gold/                     # Aggregated data (Parquet)
├── etl/
│   ├── extract.py               # Data extraction from Treasury API
│   ├── transform.py             # Data transformation logic
│   ├── transform.ipynb          # Transformation notebooks
│   └── load.py                  # Database loading functions
├── raw_data/                    # Raw CSV data from API
├── spark_jobs/
│   ├── bronze_ingest.py         # Spark job for bronze layer
│   ├── silver_transform.py      # Spark job for silver layer
│   └── gold_aggregations.py     # Spark job for gold layer
├── main.py                      # Main pipeline orchestration
├── docker-compose.yml           # Docker services configuration
├── requirements.txt             # Python dependencies
└── readme.md                    # Project documentation
```

---

## 🚀 Running the Project

### 1️⃣ Start Services

```bash
docker compose up --build
```

Services:
- Airflow UI → http://localhost:8080
- Spark UI → http://localhost:8081
- PostgreSQL → localhost:5432

---

### 2️⃣ Airflow Login

```
Username: admin
Password: admin
```

Trigger the DAG:
`treasury_data_pipeline`

---

## 🧠 Technical Highlights

- Containerized distributed Spark environment
- Medallion data lake design pattern
- Separation of compute, orchestration, and storage
- Modular ETL structure
- Production-style folder organization
- Reproducible local development environment

---

## 💡 Why This Project Matters

This project demonstrates:

- Understanding of modern data lake architecture
- Spark-based transformation pipelines
- Orchestrated workflows using Airflow
- Data warehouse loading patterns
- Real-world API ingestion handling

It reflects how real-world financial data platforms are structured.

---

## 🔮 Future Improvements

- Incremental loading logic
- Data quality validation checks
- Partitioned Parquet storage
- Star schema in Gold layer
- CI/CD integration
- Cloud deployment (AWS/GCP)

---

## 👨‍💻 Author

**Moavia Mahmood**  
Data Engineer
Focused on building scalable data systems and distributed processing pipelines.
