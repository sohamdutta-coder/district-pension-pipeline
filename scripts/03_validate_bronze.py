from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("ValidateBronzeData")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read Bronze datasets
demographics = spark.read.parquet(
    str(BRONZE_PATH / "demographics")
)

housing = spark.read.parquet(
    str(BRONZE_PATH / "housing")
)

# Check district code uniqueness
print("\nDemographics district code duplicates:")
demographics.groupBy("District code") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

print("\nHousing district code duplicates:")
housing.groupBy("District Code") \
    .count() \
    .filter(F.col("count") > 1) \
    .show()

# Check missing district identifiers
print("\nMissing district codes:")

print(
    "Demographics:",
    demographics.filter(
        F.col("District code").isNull()
    ).count()
)

print(
    "Housing:",
    housing.filter(
        F.col("District Code").isNull()
    ).count()
)

# Compare district codes between datasets
demographics_codes = demographics.select(
    F.col("District code").alias("district_code")
)

housing_codes = housing.select(
    F.col("District Code").alias("district_code")
)

print("\nDistricts missing from housing:")
demographics_codes.join(
    housing_codes,
    on="district_code",
    how="left_anti"
).show()

print("\nDistricts missing from demographics:")
housing_codes.join(
    demographics_codes,
    on="district_code",
    how="left_anti"
).show()

# Check district names and codes
print("\nDemographics district mapping:")
demographics.select(
    "District code",
    "District name"
).orderBy("District code").show(30, truncate=False)

print("\nHousing district mapping:")
housing.select(
    "District Code",
    "District Name",
    "Rural/Urban",
    "Area Name"
).orderBy("District Code").show(30, truncate=False)

# Validate population and housing indicators
print("\nNull population records:")
print(
    demographics.filter(
        F.col("Population").isNull()
    ).count()
)

print("\nNull housing household records:")
print(
    housing.filter(
        F.col("Total Number of households").isNull()
    ).count()
)

spark.stop()