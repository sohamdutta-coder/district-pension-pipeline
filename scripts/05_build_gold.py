from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

# Define project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold"

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("JharkhandPensionGoldLayer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Read Silver datasets
demographics = spark.read.parquet(
    str(SILVER_PATH / "district_demographics")
)

housing = spark.read.parquet(
    str(SILVER_PATH / "district_housing")
)

print("Silver datasets loaded successfully.")


# Validate source record counts
if demographics.count() != 24:
    raise ValueError("Expected 24 demographic records.")

if housing.count() != 24:
    raise ValueError("Expected 24 housing records.")


# Select and rename demographic fields
dem = demographics.select(
    "district_code",
    "district_name",
    "state_name",
    F.col("population").alias("total_population"),
    F.col("male").alias("male_population"),
    F.col("female").alias("female_population"),
    F.col("literate").alias("literate_population"),
    F.col("workers").alias("total_workers"),
    F.col("sc").alias("sc_population"),
    F.col("st").alias("st_population"),
    F.col("households").alias("total_households"),
    F.col("age_group_50").alias("age_50_plus_population")
)


# Select and rename housing fields
house = housing.select(
    "district_code",
    F.col("total_number_of_good").alias("good_housing_pct"),
    F.col("total_number_of_livable").alias("livable_housing_pct"),
    F.col("total_number_of_dilapidated").alias("dilapidated_housing_pct"),
    F.col("permanents").alias("permanent_housing_pct"),
    F.col("semi_permanent").alias("semi_permanent_housing_pct"),
    F.col("total_temporary").alias("temporary_housing_pct"),
    F.col("serviceable").alias("serviceable_pct"),
    F.col("non_serviceable").alias("non_serviceable_housing_pct"),
    F.col("unclassifiable").alias("unclassifiable_pct")
)


# Validate district-level source values
invalid_inputs = dem.filter(
    (F.col("total_population") <= 0)
    | (F.col("total_households") <= 0)
    | F.col("age_50_plus_population").isNull()
).count()

if invalid_inputs > 0:
    raise ValueError("Invalid demographic inputs detected.")


invalid_social_share = dem.filter(
    (F.col("sc_population") + F.col("st_population"))
    > F.col("total_population")
).count()

if invalid_social_share > 0:
    raise ValueError("Combined SC/ST population exceeds total population.")


# Join demographic and housing features
gold = dem.join(
    house,
    on="district_code",
    how="inner"
)

if gold.count() != 24:
    raise ValueError("Gold source reconciliation failed.")


# Calculate district-level indicators
gold = (
    gold
    .withColumn(
        "population_per_household",
        F.round(
            F.col("total_population") / F.col("total_households"),
            4
        )
    )
    .withColumn(
        "female_population_share_pct",
        F.round(
            100 * F.col("female_population") / F.col("total_population"),
            4
        )
    )
    .withColumn(
        "literacy_rate_pct",
        F.round(
            100 * F.col("literate_population") / F.col("total_population"),
            4
        )
    )
    .withColumn(
        "work_participation_rate_pct",
        F.round(
            100 * F.col("total_workers") / F.col("total_population"),
            4
        )
    )
    .withColumn(
        "sc_population_share_pct",
        F.round(
            100 * F.col("sc_population") / F.col("total_population"),
            4
        )
    )
    .withColumn(
        "st_population_share_pct",
        F.round(
            100 * F.col("st_population") / F.col("total_population"),
            4
        )
    )
    .withColumn(
        "social_group_population_share_pct",
        F.round(
            100 * (
                F.col("sc_population") + F.col("st_population")
            ) / F.col("total_population"),
            4
        )
    )
    .withColumn(
        "age_50_plus_population_share_pct",
        F.round(
            100 * F.col("age_50_plus_population")
            / F.col("total_population"),
            4
        )
    )
)


# Calculate minimum and maximum values for normalisation
metrics = {
    "age_50_plus_population_share_pct": "age",
    "dilapidated_housing_pct": "housing",
    "social_group_population_share_pct": "social",
    "literacy_rate_pct": "literacy",
    "work_participation_rate_pct": "work",
    "population_per_household": "household",
    "female_population_share_pct": "female"
}

for column, prefix in metrics.items():
    gold = (
        gold
        .withColumn(
            f"{prefix}_min",
            F.min(column).over(Window.partitionBy())
        )
        .withColumn(
            f"{prefix}_max",
            F.max(column).over(Window.partitionBy())
        )
    )


# Normalise vulnerability components
components = [
    (
        "age_50_plus_population_share_pct",
        "age",
        False
    ),
    (
        "dilapidated_housing_pct",
        "housing",
        False
    ),
    (
        "social_group_population_share_pct",
        "social",
        False
    ),
    (
        "literacy_rate_pct",
        "literacy",
        True
    ),
    (
        "work_participation_rate_pct",
        "work",
        True
    ),
    (
        "population_per_household",
        "household",
        False
    ),
    (
        "female_population_share_pct",
        "female",
        False
    )
]

for column, prefix, inverse in components:

    numerator = (
        F.col(f"{prefix}_max") - F.col(column)
        if inverse
        else F.col(column) - F.col(f"{prefix}_min")
    )

    denominator = (
        F.col(f"{prefix}_max") - F.col(f"{prefix}_min")
    )

    component_name = {
        "age": "age_vulnerability_component",
        "housing": "housing_vulnerability_component",
        "social": "social_vulnerability_component",
        "literacy": "literacy_vulnerability_component",
        "work": "work_vulnerability_component",
        "household": "household_pressure_component",
        "female": "female_population_component"
    }[prefix]

    gold = gold.withColumn(
        component_name,
        F.when(denominator == 0, F.lit(0.0))
        .otherwise(numerator / denominator)
    )


# Calculate weighted priority score
gold = gold.withColumn(
    "pension_outreach_priority_score",
    F.round(
        100 * (
            0.25 * F.col("age_vulnerability_component")
            + 0.20 * F.col("housing_vulnerability_component")
            + 0.15 * F.col("social_vulnerability_component")
            + 0.15 * F.col("literacy_vulnerability_component")
            + 0.10 * F.col("work_vulnerability_component")
            + 0.10 * F.col("household_pressure_component")
            + 0.05 * F.col("female_population_component")
        ),
        4
    )
)


# Rank districts and assign priority categories
ranking_window = Window.orderBy(
    F.col("pension_outreach_priority_score").desc(),
    F.col("district_code").asc()
)

gold = (
    gold
    .withColumn(
        "priority_rank",
        F.row_number().over(ranking_window)
    )
    .withColumn(
        "priority_bucket",
        F.ntile(3).over(ranking_window)
    )
    .withColumn(
        "priority_category",
        F.when(F.col("priority_bucket") == 1, "High")
        .when(F.col("priority_bucket") == 2, "Medium")
        .otherwise("Low")
    )
)


# Add metadata and unavailable pension indicators
gold = (
    gold
    .withColumn(
        "old_age_pension_beneficiary_count",
        F.lit(None).cast("long")
    )
    .withColumn(
        "widow_pension_beneficiary_count",
        F.lit(None).cast("long")
    )
    .withColumn(
        "old_age_beneficiaries_per_1000_population",
        F.lit(None).cast("double")
    )
    .withColumn(
        "widow_beneficiaries_per_1000_population",
        F.lit(None).cast("double")
    )
    .withColumn(
        "old_age_beneficiaries_per_1000_households",
        F.lit(None).cast("double")
    )
    .withColumn(
        "widow_beneficiaries_per_1000_households",
        F.lit(None).cast("double")
    )
    .withColumn(
        "total_pension_beneficiaries_per_1000_households",
        F.lit(None).cast("double")
    )
    .withColumn(
        "district_pension_data_available",
        F.lit(False)
    )
    .withColumn(
        "priority_score_method",
        F.lit("VULNERABILITY_PROXY_V1")
    )
    .withColumn(
        "census_reference_year",
        F.lit(2011)
    )
)


# Validate Gold results
total_records = gold.count()

category_counts = {
    row["priority_category"]: row["count"]
    for row in gold.groupBy("priority_category").count().collect()
}

if total_records != 24:
    raise ValueError(f"Expected 24 districts, found {total_records}.")

if category_counts != {"High": 8, "Medium": 8, "Low": 8}:
    raise ValueError(f"Unexpected priority distribution: {category_counts}")


# Validate component and score ranges
component_columns = [
    "age_vulnerability_component",
    "housing_vulnerability_component",
    "social_vulnerability_component",
    "literacy_vulnerability_component",
    "work_vulnerability_component",
    "household_pressure_component",
    "female_population_component"
]

for column in component_columns:
    invalid_count = gold.filter(
        (F.col(column) < 0) | (F.col(column) > 1)
    ).count()

    if invalid_count > 0:
        raise ValueError(f"Invalid values in {column}.")


invalid_scores = gold.filter(
    (F.col("pension_outreach_priority_score") < 0)
    | (F.col("pension_outreach_priority_score") > 100)
).count()

if invalid_scores > 0:
    raise ValueError("Priority scores are outside the expected range.")


# Remove temporary normalisation columns
temporary_columns = [
    f"{prefix}_{bound}"
    for prefix in metrics.values()
    for bound in ["min", "max"]
]

gold = gold.drop(*temporary_columns)


# Arrange output by priority rank
gold = gold.orderBy("priority_rank")


# Save Gold mart as Parquet
gold.write \
    .mode("overwrite") \
    .parquet(str(GOLD_PATH / "mart_district_pension_outreach"))


print("\nGold Layer Validation")
print("Total district records:", total_records)
print("Priority distribution:", category_counts)
print("Invalid scores:", invalid_scores)
print("District pension data available: 0")

print("\nGold mart generated successfully.")

# Display priority results
gold.select(
    "district_code",
    "district_name",
    "pension_outreach_priority_score",
    "priority_rank",
    "priority_category"
).show(24, truncate=False)

# Close Spark session
spark.stop()