from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_ROOT / "data" / "raw"
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("JharkhandPensionBronzeIngestion")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read source CSV files
demographics = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(RAW_PATH / "india-districts-census-2011.csv"))
)

housing = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(RAW_PATH / "hlpca-total.csv"))
)

nsap = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(RAW_PATH / "RS_Session_256_AU_3148_1.csv"))
)

print("\nSource datasets loaded successfully.")

# Filter records for Jharkhand
jharkhand_demographics = demographics.filter(
    F.upper(F.trim(F.col("State name"))) == "JHARKHAND"
)

jharkhand_housing = housing.filter(
    F.upper(F.trim(F.col("State Name"))) == "JHARKHAND"
)

jharkhand_nsap = nsap.filter(
    F.upper(F.trim(F.col("States/UTs"))) == "JHARKHAND"
)

# Display record counts
print("\nRecord Counts")

print("Demographics:", jharkhand_demographics.count())
print("Housing:", jharkhand_housing.count())
print("NSAP:", jharkhand_nsap.count())

# Display dataset schemas
print("\nDemographics Schema")
jharkhand_demographics.printSchema()

print("\nHousing Schema")
jharkhand_housing.printSchema()

print("\nNSAP Schema")
jharkhand_nsap.printSchema()

# Save filtered datasets in Parquet format
jharkhand_demographics.write \
    .mode("overwrite") \
    .parquet(str(BRONZE_PATH / "demographics"))

jharkhand_housing.write \
    .mode("overwrite") \
    .parquet(str(BRONZE_PATH / "housing"))

jharkhand_nsap.write \
    .mode("overwrite") \
    .parquet(str(BRONZE_PATH / "nsap"))

print("\nBronze ingestion completed successfully.")

# Close Spark session
spark.stop()