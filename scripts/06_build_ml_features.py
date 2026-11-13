import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]

GOLD_PATH = PROJECT_ROOT / "data" / "gold"
ML_PATH = PROJECT_ROOT / "data" / "ml"
OUTPUT_PATH = PROJECT_ROOT / "output"

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("JharkhandPensionMLFeatures")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Read Gold mart
gold = spark.read.parquet(
    str(GOLD_PATH / "mart_district_pension_outreach")
)

print("Gold mart loaded successfully.")


# Define ML feature columns
feature_columns = [
    "district_code",
    "district_name",
    "census_reference_year",

    "total_population",
    "male_population",
    "female_population",
    "literate_population",
    "total_workers",
    "sc_population",
    "st_population",
    "total_households",

    "population_per_household",
    "female_population_share_pct",
    "literacy_rate_pct",
    "work_participation_rate_pct",
    "sc_population_share_pct",
    "st_population_share_pct",
    "social_group_population_share_pct",

    "age_50_plus_population",
    "age_50_plus_population_share_pct",

    "good_housing_pct",
    "livable_housing_pct",
    "dilapidated_housing_pct",
    "permanent_housing_pct",
    "semi_permanent_housing_pct",
    "temporary_housing_pct",
    "serviceable_pct",
    "non_serviceable_housing_pct",
    "unclassifiable_pct",

    "old_age_pension_beneficiary_count",
    "widow_pension_beneficiary_count",
    "old_age_beneficiaries_per_1000_population",
    "widow_beneficiaries_per_1000_population",
    "old_age_beneficiaries_per_1000_households",
    "widow_beneficiaries_per_1000_households",
    "total_pension_beneficiaries_per_1000_households",
    "district_pension_data_available",

    "priority_category",
    "priority_rank",
    "pension_outreach_priority_score",
    "priority_score_method"
]

# Select features from Gold
ml_features = gold.select(*feature_columns)


# Generate binary target
ml_features = (
    ml_features
    .withColumn(
        "target_high_priority",
        F.when(F.col("priority_category") == "High", 1)
        .otherwise(0)
    )
    .withColumnRenamed(
        "priority_category",
        "target_priority_category"
    )
    .withColumnRenamed(
        "priority_rank",
        "target_priority_rank"
    )
    .withColumnRenamed(
        "pension_outreach_priority_score",
        "target_priority_score"
    )
    .withColumn(
        "target_definition",
        F.lit(
            "Rule-based proxy label: 1 = High priority; "
            "0 = Medium or Low priority. Derived from Gold "
            "priority classification, not observed pension "
            "enrolment or outreach outcomes."
        )
    )
)


# Validate district records
total_records = ml_features.count()

unique_districts = (
    ml_features
    .select("district_code")
    .distinct()
    .count()
)

if total_records != 24 or unique_districts != 24:
    raise ValueError("District count or uniqueness validation failed.")


# Validate target distribution
target_distribution = {
    row["target_priority_category"]: row["count"]
    for row in ml_features.groupBy("target_priority_category").count().collect()
}

if target_distribution != {"High": 8, "Medium": 8, "Low": 8}:
    raise ValueError(f"Unexpected target distribution: {target_distribution}")


# Validate binary target
invalid_targets = ml_features.filter(
    ~F.col("target_high_priority").isin(0, 1)
).count()

if invalid_targets > 0:
    raise ValueError("Invalid binary target values detected.")


# Validate pension data availability
available_pension = ml_features.filter(
    F.col("district_pension_data_available") != False
).count()

pension_columns = [
    "old_age_pension_beneficiary_count",
    "widow_pension_beneficiary_count",
    "old_age_beneficiaries_per_1000_population",
    "widow_beneficiaries_per_1000_population",
    "old_age_beneficiaries_per_1000_households",
    "widow_beneficiaries_per_1000_households",
    "total_pension_beneficiaries_per_1000_households"
]

for column in pension_columns:
    non_null_count = ml_features.filter(
        F.col(column).isNotNull()
    ).count()

    if non_null_count > 0:
        raise ValueError(f"Unexpected pension values in {column}.")

if available_pension > 0:
    raise ValueError("Unexpected district-level pension availability.")


# Save ML feature table
ml_features.write \
    .mode("overwrite") \
    .parquet(str(ML_PATH / "ml_district_features"))

# Export CSV
ml_features.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .option("emptyValue", "") \
    .csv(str(ML_PATH / "ml_district_features_csv"))


print("\nML Feature Validation")
print("Total records:", total_records)
print("Unique districts:", unique_districts)
print("Priority distribution:", target_distribution)
print("Invalid targets:", invalid_targets)
print("District-level pension data available:", available_pension)

print("\nML feature table generated successfully.")

# Display sample records
ml_features.select(
    "district_name",
    "target_priority_category",
    "target_high_priority",
    "target_priority_rank",
    "target_priority_score"
).show(24, truncate=False)

# Close Spark session
spark.stop()

# Export final ML feature CSV
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

csv_part = next(
    (ML_PATH / "ml_district_features_csv").glob("part-*.csv")
)

shutil.copy2(
    csv_part,
    OUTPUT_PATH / "ml_district_features.csv"
)

print("CSV exported successfully:", OUTPUT_PATH / "ml_district_features.csv")
