📊 Reproducible Batch Data Pipeline with Spark, Airflow & PostgreSQL

Project Overview : 

This project implements an end-to-end batch-processing data architecture for large-scale intraday stock market data. 

The system ingests 1-minute OHLC data, performs distributed rolling analytics using Apache Spark, and generates ML-ready crash detection features.

The entire pipeline is containerized using Docker and orchestrated via Apache Airflow, ensuring reproducibility, reliability, and governance-aware execution.

🏗 Architecture

The system follows a microservices-based batch architecture:

CSV Dataset

    ↓
    
Ingestion Service (Python + COPY)

    ↓
    
PostgreSQL (Raw Storage)

    ↓
    
Spark Batch Analytics

    ↓
    
PostgreSQL (Feature Store)

    ↓
    
Verification & Metadata Layer


Core Components

Layer	                                            Technology	                             Purpose
Orchestration	                                    Apache Airflow	                         DAG scheduling & monitoring
Ingestion		                         	            Python + psycopg2		                     Deterministic CSV ingestion
Raw Storage		                         	          PostgreSQL		                           Persistent raw data store
Processing		                                    Apache Spark		                         Distributed rolling analytics
Feature Store		                                  PostgreSQL		                           ML-ready analytics table
Runtime		                         	              Docker Compose		                       Infrastructure as Code



⚙ Key Engineering Features

🔹 Deterministic Ingestion

•	SHA256 dataset fingerprinting

•	Guardrails preventing duplicate ingestion

•	Transaction-safe bulk loading via PostgreSQL COPY

•	Row-count read-back validation

•	Idempotent execution

🔹 Distributed Rolling Analytics

•	Window-based momentum and volatility metrics

•	VWAP and drawdown computation

•	Composite crash index construction

•	Automatic threshold selection (F1-score optimized)

🔹 Data Integrity & Governance

•	Duplicate timestamp detection

•	Time-gap validation (1-minute consistency)

•	Null and anomaly checks

•	Deterministic MD5 checksum on analytics output

•	SQL-level verification layer

🔹 Drift Detection

•	Statistical Z-score-based drift monitoring

•	Historical run comparison

•	Metadata persistence per pipeline execution

🔹 Reliability & Performance

•	Exponential retry mechanisms

•	Transaction rollback protection

•	PostgreSQL WAL tuning for bulk writes

•	Spark partitioned JDBC writes

•	Controlled container memory limits

📂 Project Structure

Airflow_batch-stock-market-pipeline/

│

├── airflow/

│   ├── dags/

│   │   └── stock_batch_pipeline_dag.py

│   └── logs/

│

├── ingestion/

│   ├── Dockerfile

│   ├── ingest.py

│   └── requirements.txt

│

├── data/

│   ├── 1_min_SPY_2008-2021.csv

│

├── spark/

│   ├── Dockerfile

│   ├── entrypoint.sh

│   └── batch_job.py

│

├── db/

│   ├── 01_create_stockdb.sql

│   └── 02_stock_schema.sql

│

├── docker-compose.yml

├── .env

└── .dockerignore

🚀 How to Run

1️⃣ Prerequisites

Docker

Docker Compose

Minimum 8GB RAM recommended


4️⃣ Trigger the Pipeline
DAG name:
stock_batch_pipeline
Pipeline stages:
1.	wait_for_postgres
2.	run_ingestion
3.	run_spark
4.	verify_analytics
5.	finalize_pipeline
Successful execution confirms end-to-end consistency.
________________________________________
🗄 Database Tables
Raw Layer
•	raw_stock_prices
Feature Layer
•	stock_intraday_features
Metadata Tables
•	ingestion_metadata
•	pipeline_run_metadata
________________________________________
🔐 Reproducibility
This system is fully reproducible through:
•	Docker Compose
•	Environment-based configuration
•	SQL initialization scripts
•	Version-controlled infrastructure
•	Deterministic dataset fingerprinting
Running the pipeline on another machine produces identical analytical results.
________________________________________
📈 Performance Optimizations
•	WAL & checkpoint tuning in PostgreSQL
•	Chunked ingestion (200k rows per batch)
•	Spark shuffle partition tuning
•	JDBC batch size optimization
•	Memory allocation controls
________________________________________
⚠ Limitations
•	Single-node Spark execution
•	Manual DAG trigger (batch-only mode)
•	No cloud deployment layer
•	No horizontal database scaling
________________________________________
🔮 Future Enhancements
•	Real-time streaming pipeline (Kafka + Spark Structured Streaming)
•	Kubernetes deployment
•	CI/CD integration
•	Automated data quality dashboards
•	Cloud-native scaling

# 📊 Reproducible Batch Data Pipeline with Spark, Airflow & PostgreSQL

## 📌 Project Overview

This project implements an end-to-end **batch-processing data architecture** for large-scale intraday stock market data. The system ingests 1-minute OHLC data, performs distributed rolling analytics using Apache Spark, and generates ML-ready crash detection features.

The architecture is fully containerized using Docker, orchestrated via Apache Airflow, and designed with deterministic ingestion, validation, metadata lineage tracking, and statistical drift detection.

The system demonstrates production-style reliability, reproducibility, and governance-aware batch execution.

---

# 🏗 Architecture

The pipeline follows a microservices-based batch architecture:

CSV Dataset
↓
Ingestion Service (Python + COPY)
↓
PostgreSQL (Raw Storage)
↓
Spark Batch Analytics
↓
PostgreSQL (Feature Store)
↓
Verification & Metadata Layer


## Core Components

| Layer | Technology | Responsibility |
|--------|------------|----------------|
| Orchestration | Apache Airflow | DAG scheduling & monitoring |
| Ingestion | Python + psycopg2 | Deterministic CSV ingestion |
| Raw Storage | PostgreSQL | Persistent raw data store |
| Processing | Apache Spark | Distributed rolling analytics |
| Feature Store | PostgreSQL | ML-ready analytics features |
| Runtime | Docker Compose | Infrastructure as Code |

---

# ⚙ Key Engineering Features

## 🔹 Deterministic & Idempotent Ingestion
- SHA256 dataset fingerprinting
- Guardrails preventing duplicate ingestion
- Transaction-safe bulk loading (PostgreSQL COPY)
- Row-count read-back validation
- Controlled table truncation

## 🔹 Distributed Rolling Analytics
- Intraday window functions (5–60 minute rolling metrics)
- Volatility normalization (Z-score)
- VWAP and drawdown computation
- Composite crash index construction
- Automatic F1-score optimized threshold selection

## 🔹 Data Integrity & Governance
- Duplicate timestamp detection
- Time-gap validation (1-minute continuity)
- Logical consistency checks
- Deterministic MD5 checksum of feature table
- SQL-based verification layer

## 🔹 Statistical Drift Detection
- Z-score-based drift monitoring
- Historical run comparison
- Metadata persistence per pipeline execution

## 🔹 Reliability & Performance Engineering
- Exponential retry mechanisms
- Transaction rollback protection
- PostgreSQL WAL tuning
- Partitioned JDBC writes
- Container memory isolation

---

# 📂 Project Structure

Airflow_batch-stock-market-pipeline/
│
├── airflow/
│ ├── dags/
│ │ └── stock_batch_pipeline_dag.py
│ └── logs/
│
├── ingestion/
│ ├── Dockerfile
│ ├── ingest.py
│ └── requirements.txt
│
├── spark/
│ ├── Dockerfile
│ ├── entrypoint.sh
│ └── batch_job.py
│
├── db/
│ ├── 01_create_stockdb.sql
│ └── 02_stock_schema.sql
│
├── docker-compose.yml
├── .env
└── .dockerignore


---

# 🔧 Environment Configuration

Create a `.env` file in the project root:

Airflow Metadata Database

AIRFLOW_DB=airflowdb
Business PostgreSQL Database

STOCK_DB=stockdb
POSTGRES_USER=stockuser
POSTGRES_PASSWORD=stockpass
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
Airflow Postgres Connection

AIRFLOW_CONN_POSTGRES_DEFAULT=postgres://stockuser:stockpass@postgres:5432/stockdb
Dataset Path (mounted volume)

CSV_PATH=/data/1_min_SPY_2008-2021.csv
Spark Runtime Configuration

SPARK_CONF=--conf spark.executor.memory=4g
--conf spark.driver.memory=4g
--conf spark.sql.shuffle.partitions=8
--conf spark.memory.fraction=0.7
--conf spark.sql.codegen.aggregate.map.twolevel.enabled=false


> All configuration is environment-driven. No credentials are hardcoded inside application source code.

---

# 💻 System Requirements

- Docker
- Docker Compose
- Minimum 8GB RAM recommended
- Port 8080 available

---

# 🚀 How to Run

## Step 1 – Clean Start (Recommended First Run)

docker compose down -v

Step 2 – Build & Start Infrastructure

docker compose up --build

This initializes:

    PostgreSQL

    Dataset volume seeding

    Airflow metadata DB

    Airflow admin user

    Airflow webserver & scheduler

🌐 Access Airflow

Open:

http://localhost:8080

Login:

Username: admin
Password: admin

▶ Trigger the Pipeline

DAG name:

stock_batch_pipeline

Execution stages:

    wait_for_postgres

    run_ingestion

    run_spark

    verify_analytics

    finalize_pipeline

✅ Verification Checklist

A successful run should show:
Ingestion

Verification status : SUCCESS

Spark

Spark job completed successfully

Database Verification

DB VERIFICATION COMPLETED SUCCESSFULLY

Finalization

PIPELINE COMPLETED SUCCESSFULLY

🗄 Database Tables
Raw Layer

    raw_stock_prices

Feature Layer

    stock_intraday_features

Metadata Tables

    ingestion_metadata

    pipeline_run_metadata

🔄 Re-running the Pipeline

The pipeline supports idempotent execution:

    Duplicate dataset ingestion prevented

    Spark overwrites feature table safely

    Metadata updated per run

    Drift comparison executed automatically

📈 Performance Optimizations

    WAL & checkpoint tuning (PostgreSQL)

    Chunked ingestion (200k rows per batch)

    Spark shuffle partition tuning

    Partitioned JDBC batch writes

    Controlled container memory allocation

⚠ Limitations

    Single-node Spark execution

    Manual DAG trigger (batch mode only)

    No horizontal database scaling

    Local deployment only

🔮 Future Enhancements

    Real-time streaming extension (Kafka + Spark Structured Streaming)

    Kubernetes deployment

    CI/CD integration

    Automated data quality dashboards

    Cloud-native scaling

👨‍💻 Author
