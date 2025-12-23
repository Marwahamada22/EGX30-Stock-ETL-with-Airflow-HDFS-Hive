# EGX30-Stock-ETL-with-Airflow-HDFS-Hive
Automated daily data pipeline to extract EGX30 stock prices, validate them, and load them into Hive using Apache Airflow.


<img width="1200" height="630" alt="image" src="https://github.com/user-attachments/assets/a8fa0b30-3bde-4e63-a410-7f84847b32fd" />

EGX30 Stock Data Engineering Pipeline

Building a Production-Style Financial Data Pipeline using Apache Airflow & Hive

🌍 Project Motivation

Financial data is highly time-sensitive and requires reliable, automated pipelines to ensure accuracy and availability for analytics.
In this project, I designed and implemented a production-style data engineering pipeline to automate the daily ingestion, validation, and storage of Egyptian stock market data (EGX30).

The goal was not only to extract data, but to apply data engineering best practices such as orchestration, fault tolerance, data validation, and partitioned data warehousing.

🧠 Project Overview

The EGX30 Stock Data Engineering Pipeline is a scheduled Airflow DAG that runs daily to:

Collect EGX30 stock prices from an external API (Yahoo Finance)

Validate and clean incoming data

Prepare structured datasets for analytics

Load curated data into Apache Hive using optimized storage formats

The pipeline ensures that historical stock data is stored efficiently and is always ready for downstream reporting or analysis.

⚙️ System Architecture (High-Level)

Data Source → Airflow → Validation Layer → Hive Staging → Hive Final Table

Data Source: Yahoo Finance API

Orchestration: Apache Airflow

Processing: Python

Storage: Apache Hive (ORC, Partitioned)

This architecture mirrors real-world data warehouse ingestion pipelines used in financial and analytics platforms.

🔄 Pipeline Workflow
🔹 Task 1: Extract Stock Data

Fetches daily stock prices for selected EGX30 symbols

Handles API failures using retries and logging

Outputs data into a structured CSV file

Shares metadata using Airflow XComs

🔹 Task 2: Data Validation & Preparation

Validates data types and record structure

Removes duplicate stock symbols

Filters invalid or missing price values

Prepares clean data files for Hive ingestion

Dynamically generates Hive SQL (HQL) scripts

🔹 Task 3: Load Data into Hive

Loads validated data into a staging table

Inserts data into a partitioned ORC table

Drops and recreates partitions to avoid duplicates

Ensures historical consistency per trade date

🧱 Data Modeling & Storage Strategy
📌 Staging Layer

TEXTFILE format

Used for raw ingestion and validation

📌 Final Layer

ORC format for performance optimization

Partitioned by trade_date

Designed for efficient querying and historical analysis

This layered approach follows standard Data Warehouse & Lakehouse ingestion patterns.

🛠 Tools & Technologies

Apache Airflow – DAG orchestration & scheduling

Python – Data extraction, validation, and transformation

Apache Hive – Data warehousing & SQL analytics

HDFS / Local FS – Intermediate storage

Yahoo Finance API – External data source

✨ Key Engineering Features

Daily automated scheduling

Retry and failure handling

XCom-based task communication

Dynamic Hive partition management

Separation of staging and curated layers

Scalable design for adding new symbols or markets

🚀 Possible Extensions

Store raw data in HDFS (Bronze layer)

Add data quality checks (row count, null checks)

Integrate with Spark for large-scale processing

Expose data to BI tools (Power BI / Tableau)

Add monitoring and alerting via Airflow callbacks

🏁 Conclusion

This project demonstrates my ability to design and implement end-to-end data engineering pipelines using modern data tools.
It reflects real production concepts such as orchestration, data validation, and warehouse optimization, making it suitable as a strong portfolio project for Data Engineering and Analytics roles.

