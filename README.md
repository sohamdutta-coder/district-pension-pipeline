# Jharkhand Pension Outreach Pipeline

A local data engineering project built using PySpark and Apache Airflow.

The project processes Census and NSAP pension-related datasets to prepare
district-level data for pension outreach prioritisation.

## Technology Stack

- Python
- PySpark
- Apache Airflow
- Pandas
- Parquet
- Git

## Pipeline Architecture

The pipeline follows a Bronze-Silver-Gold architecture.

- Bronze: Ingest and store source datasets.
- Silver: Clean, standardise and validate data.
- Gold: Prepare district-level analytical datasets.
- ML: Prepare features for district-level prioritisation.

## Project Structure

```text
district-pension-pipeline/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── ml/
├── scripts/
├── dags/
├── logs/
├── first_spark.py
├── requirements.txt
└── README.md