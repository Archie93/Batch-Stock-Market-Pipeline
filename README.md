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
