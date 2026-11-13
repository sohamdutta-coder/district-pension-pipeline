# Jharkhand District Pension Outreach Pipeline

An end-to-end data engineering pipeline built using **Apache PySpark** and **Apache Airflow** to process district-level demographic and housing data for pension outreach prioritisation in Jharkhand, India.

The project follows a **Bronze–Silver–Gold architecture**, transforming raw census and pension-related datasets into structured analytical datasets and an ML-ready feature table. Apache Airflow orchestrates the complete pipeline through a sequence of dependent tasks.

## 1. Project Overview

The objective of this project is to develop a data pipeline that integrates demographic, housing and pension-related information to support district-level pension outreach prioritisation.

The pipeline processes Census 2011 demographic and housing information for all 24 districts of Jharkhand. It calculates demographic and socio-economic indicators, derives a vulnerability-based prioritisation score, and generates a structured dataset for further machine learning exploration.

Since the available NSAP pension dataset contains Jharkhand-level figures rather than district-level beneficiary records, the project does not distribute or estimate state-level pension figures across individual districts. Instead, district prioritisation is based on demographic and housing vulnerability indicators.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Programming language |
| Apache PySpark 4.3.0 | Distributed data processing and transformation |
| Apache Airflow 3.4.0 | Workflow orchestration and task scheduling |
| Java 17 | Runtime environment for Apache Spark |
| Pandas / CSV | Source data handling and inspection |
| Parquet | Storage format for processed datasets |
| CSV | Export format for the ML feature dataset |

## 3. Data Sources

The pipeline uses the following datasets:

### 3.1 Census District Demographics

**Source file:** `india-districts-census-2011.csv`

Contains district-level demographic information, including:

- Total population
- Male and female population
- Scheduled Caste (SC) population
- Scheduled Tribe (ST) population
- Literacy
- Total workers
- Population belonging to different age groups

### 3.2 Census District Housing

**Source file:** `hlpca-total.csv`

Contains district-level household and housing information, including:

- Total number of households
- Household amenities
- Housing conditions
- Rural and urban household statistics

The pipeline uses the total-area records for district-level integration.

### 3.3 NSAP Pension Data

**Source file:** `RS_Session_256_AU_3148_1.csv`

Contains pension scheme-related information from the National Social Assistance Programme (NSAP).

The supplied dataset provides state-level information for Jharkhand. District-level pension beneficiary figures are not available in the source data and are therefore retained as NULL in the district-level output.

## 4. Pipeline Architecture

The project follows a Bronze–Silver–Gold data architecture, followed by ML feature generation.

### 4.1 Bronze Layer – Raw Data Ingestion

The Bronze layer reads the original CSV files, filters the relevant Jharkhand records where applicable, and stores the ingested datasets in Parquet format.

Datasets generated:

- `demographics`
- `housing`
- `nsap`

### 4.2 Silver Layer – Data Cleaning and Integration

The Silver layer standardises column names, converts district codes into a consistent format, and prepares the datasets for integration.

Demographic and housing datasets are joined using district codes. The NSAP dataset is maintained separately because it contains state-level information.

Datasets generated:

- `district_demographics`
- `district_housing`
- `state_nsap`
- `district_integrated`

### 4.3 Gold Layer – Analytical Data Mart

The Gold layer generates the district-level pension outreach prioritisation mart.

It integrates demographic and housing indicators and calculates:

- Population per household
- Female population share
- Literacy rate
- Work participation rate
- SC population share
- ST population share
- Social group population share
- Population aged 50 years and above

The pipeline then normalises selected indicators using min-max normalisation and calculates a weighted vulnerability-based priority score.

| Indicator | Weight |
|---|---:|
| Age-related vulnerability | 25% |
| Housing vulnerability | 20% |
| Social group vulnerability | 15% |
| Literacy vulnerability | 15% |
| Work participation vulnerability | 10% |
| Household pressure | 10% |
| Female population share | 5% |
| **Total** | **100%** |

Based on the calculated score, districts are ranked and assigned to three priority categories:

- High Priority
- Medium Priority
- Low Priority

The output contains 24 district records, with eight districts in each category.

### 4.4 ML Feature Layer

The final stage prepares the Gold mart for machine learning exploration.

It selects relevant demographic, housing and calculated indicators, creates a binary `target_high_priority` column, and exports the resulting feature dataset in both Parquet and CSV formats.

The target is derived from the rule-based Gold classification. It is therefore a proxy label, not an independently observed pension outreach outcome.

## 5. Project Structure

```text
district-pension-pipeline/
│
├── dags/
│   └── district_pension_pipeline.py
│
├── scripts/
│   ├── 01_ingest_bronze.py
│   ├── 02_inspect_bronze.py
│   ├── 03_validate_bronze.py
│   ├── 04_build_silver.py
│   ├── 05_build_gold.py
│   └── 06_build_ml_features.py
│
├── data/
│   ├── raw/
│   │   ├── india-districts-census-2011.csv
│   │   ├── hlpca-total.csv
│   │   └── RS_Session_256_AU_3148_1.csv
│   │
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── ml/
│
├── output/
│
├── logs/
├── first_spark.py
├── requirements.txt
├── README.md
└── venv/
```

The `data/` directory contains the input datasets and generated outputs. Processed datasets are stored in Parquet format, while the ML feature dataset is also exported as CSV.

The virtual environment and generated data directories are local development artefacts and need not be committed to version control.

## 6. Environment Setup

### Prerequisites

Install the following:

- Python 3.11
- Java 17
- Apache Airflow 3.4.0
- Apache PySpark 4.3.0

Verify the installations:

```bash
python3 --version
java -version
```

### 6.1 Clone the Repository

```bash
git clone <repository-url>
cd district-pension-pipeline
```

### 6.2 Create and Activate the Python Environment

```bash
python3.11 -m venv venv
source venv/bin/activate
```

### 6.3 Install PySpark

```bash
pip install pyspark==4.3.0
```

### 6.4 Configure Java

Set the Java 17 environment variables:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

Verify:

```bash
echo $JAVA_HOME
java -version
```

### 6.5 Install Apache Airflow

For this project, Airflow is maintained in a separate virtual environment.

```bash
python3.11 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
```

Install Airflow with its matching constraints:

```bash
pip install "apache-airflow==3.4.0" \
--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.11.txt"
```

Verify:

```bash
airflow version
```

## 7. Running the Pipeline

### 7.1 Execute PySpark Scripts Individually

Activate the project environment:

```bash
cd district-pension-pipeline
source venv/bin/activate
```

Execute the scripts in the following order:

```bash
python scripts/01_ingest_bronze.py
python scripts/03_validate_bronze.py
python scripts/04_build_silver.py
python scripts/05_build_gold.py
python scripts/06_build_ml_features.py
```

Each script generates the corresponding output in the `data/` directory.

### 7.2 Run the Pipeline Using Apache Airflow

The Airflow DAG is defined in:

`dags/district_pension_pipeline.py`

The DAG contains five BashOperator tasks connected through sequential dependencies.

```text
ingest_bronze
      |
      v
validate_bronze
      |
      v
build_silver
      |
      v
build_gold
      |
      v
build_ml_features
```

The DAG is configured for manual execution using `schedule=None`.

Set the DAG directory:

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$HOME/Documents/Projects/district-pension-pipeline/dags"
```

Start the Airflow services in separate terminal windows.

**Terminal 1 – API Server**

```bash
source ~/airflow-venv/bin/activate
airflow api-server
```

**Terminal 2 – Scheduler**

```bash
source ~/airflow-venv/bin/activate
airflow scheduler
```

**Terminal 3 – DAG Processor**

```bash
source ~/airflow-venv/bin/activate
airflow dag-processor
```

Open the Airflow web interface:

`http://localhost:8080`

Locate `district_pension_pipeline` and click **Trigger** to execute the workflow.

Task execution, logs, status and DAG run history can be monitored through the Airflow interface.

## 8. Data Validation

The pipeline includes validation checks at different stages.

The Bronze validation stage checks:

- Number of district records
- Duplicate district codes
- Missing district codes
- Null population values
- Null household values
- Consistency of district codes across datasets

The Gold and ML stages include additional checks for:

- Expected district record count
- Unique district codes
- Valid priority categories
- Priority score ranges
- Priority distribution
- Availability of district-level pension data

## 9. Project Results

The completed pipeline successfully processes the 24 districts of Jharkhand.

| Stage | Output | Records |
|---|---|---:|
| Bronze | Demographics | 24 |
| Bronze | Housing | 24 |
| Bronze | NSAP | State-level records |
| Gold | District Pension Outreach Mart | 24 |
| ML | District ML Features | 24 |

The Gold mart classifies the districts into three priority categories:

| Priority Category | Number of Districts |
|---|---:|
| High | 8 |
| Medium | 8 |
| Low | 8 |
| **Total** | **24** |

The final ML feature dataset is available in:

- `data/ml/ml_district_features`
- `data/ml/ml_district_features_csv`

The CSV output is generated as a Spark output directory containing a CSV part file and a `_SUCCESS` marker.

### Airflow Execution

The complete Airflow DAG was tested successfully.

- Total tasks: 5
- Successful tasks: 5
- Failed tasks: 0
- Approximate execution time: 46 seconds

## 10. Important Considerations

1. The demographic and housing datasets are based on Census 2011 and should not be interpreted as current population statistics.
2. The supplied NSAP dataset contains state-level information, not district-level pension beneficiary figures.
3. District-level pension beneficiary fields are intentionally retained as NULL.
4. The prioritisation score is a rule-based vulnerability proxy based on selected demographic and housing indicators.
5. The ML target is derived from the rule-based priority classification. It should not be treated as an independently verified prediction target.
6. The current implementation is a local development setup and uses manually triggered Airflow execution.

## 11. Future Enhancements

- Integrate more recent district-level demographic and pension beneficiary datasets.
- Introduce automated data quality monitoring.
- Develop additional analytical dashboards.
- Explore machine learning models using independently observed outcome labels.
- Configure scheduled pipeline execution.
- Deploy the pipeline in a production environment.

## 12. Conclusion

This project demonstrates an end-to-end data engineering workflow using Apache PySpark and Apache Airflow. It covers raw data ingestion, data validation, transformation, analytical data mart generation and ML feature preparation.

The implementation provides a foundation for further exploration of large-scale data processing, workflow orchestration and data-driven district-level outreach prioritisation.
