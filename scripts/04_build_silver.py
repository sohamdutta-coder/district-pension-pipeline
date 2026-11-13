from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import re

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("JharkhandPensionSilverLayer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Standardise column names
def standardise_columns(df):
    for column in df.columns:
        new_name = re.sub(r"[^a-zA-Z0-9]+", "_", column.strip())
        new_name = new_name.strip("_").lower()

        if column != new_name:
            df = df.withColumnRenamed(column, new_name)

    return df


# Read Bronze datasets
demographics = spark.read.parquet(
    str(BRONZE_PATH / "demographics")
)

housing = spark.read.parquet(
    str(BRONZE_PATH / "housing")
)

nsap = spark.read.parquet(
    str(BRONZE_PATH / "nsap")
)

print("Bronze datasets loaded successfully.")


# Standardise column names
demographics = standardise_columns(demographics)
housing = standardise_columns(housing)
nsap = standardise_columns(nsap)


# Standardise district codes
demographics = demographics.withColumn(
    "district_code",
    F.col("district_code").cast("string")
)

housing = housing.withColumn(
    "district_code",
    F.col("district_code").cast("string")
)


# Resolve duplicate column names before joining
demographic_columns = demographics.columns

for column in housing.columns:
    if column != "district_code" and column in demographic_columns:
        housing = housing.withColumnRenamed(
            column,
            f"housing_{column}"
        )


# Join demographics and housing
district_integrated = demographics.join(
    housing,
    on="district_code",
    how="inner"
)

print("\nIntegrated district dataset created.")


# Validate integrated dataset
total_records = district_integrated.count()

duplicate_records = (
    district_integrated
    .groupBy("district_code")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_district_codes = district_integrated.filter(
    F.col("district_code").isNull()
).count()

print("\nSilver Layer Validation")
print("Total district records:", total_records)
print("Duplicate district codes:", duplicate_records)
print("Missing district codes:", missing_district_codes)


# Save standardised Silver datasets
demographics.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH / "district_demographics"))

housing.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH / "district_housing"))

nsap.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH / "state_nsap"))

district_integrated.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH / "district_integrated"))

print("\nSilver layer generated successfully.")

# Close Spark session
spark.stop()