from pyspark.sql import SparkSession
from pathlib import Path

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("InspectBronzeData")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

datasets = ["demographics", "housing", "nsap"]

# Inspect each Bronze dataset
for dataset in datasets:
    print(f"\nDataset: {dataset}")

    df = spark.read.parquet(str(BRONZE_PATH / dataset))

    print("Total records:", df.count())

    print("\nColumns:")
    print(df.columns)

    print("\nSchema:")
    df.printSchema()

    print("\nSample records:")
    df.show(5, truncate=False)

spark.stop()