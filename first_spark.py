from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("DistrictPensionPipeline")
    .master("local[*]")
    .getOrCreate()
)

# Sample district data
data = [
    ("Ranchi", 2914253, 342035),
    ("Dhanbad", 2684487, 391000),
    ("Bokaro", 2062330, 290000),
    ("Deoghar", 1492073, 180000)
]

columns = ["district", "population", "sc_population"]

# Create Spark DataFrame
df = spark.createDataFrame(data, columns)

print("Original Dataset:")
df.show()

# Calculate SC population percentage
df = df.withColumn(
    "sc_percentage",
    round((col("sc_population") / col("population")) * 100, 2)
)

print("Dataset with SC Population Percentage:")
df.show()

df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/district_analysis")

# Stop Spark session
spark.stop()