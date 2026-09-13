# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,DLT Pipeline - Bronze/Silver/Gold
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================================================================
# 0. CONSTANTS & VOLUME LANDING PATHS
# ==============================================================================
CATALOG = "rearc"
SCHEMA = "default"
VOLUME = "rearc_landing_zone"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
BLS_RAW_PATH = f"{VOLUME_PATH}/bls_productivity"
POP_RAW_PATH = f"{VOLUME_PATH}/us_population/population_data.json"

# Helper Function: Cleans trailing/leading whitespaces from raw file headers dynamically
def clean_bronze_headers(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip())
    return df

# ==============================================================================
# 1. EXPANDED BRONZE LAYER (Ingesting All Structurally Tabular Datasets)
# ==============================================================================

@dlt.table(name="bronze_bls_data", comment="Raw fact observations from pr.data.0.Current")
def bronze_bls_data():
    return clean_bronze_headers(spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","\t").load(f"{BLS_RAW_PATH}/pr.data.0.Current"))

@dlt.table(name="bronze_bls_series", comment="Raw dimension definitions from pr.series")
def bronze_bls_series():
    return clean_bronze_headers(spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","\t").load(f"{BLS_RAW_PATH}/pr.series"))

@dlt.table(name="bronze_bls_sector", comment="Raw dimensional tracking maps from pr.sector")
def bronze_bls_sector():
    return clean_bronze_headers(spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","\t").load(f"{BLS_RAW_PATH}/pr.sector"))

@dlt.table(name="bronze_bls_measure", comment="Raw dimensional measurement types from pr.measure")
def bronze_bls_measure():
    return clean_bronze_headers(spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","\t").load(f"{BLS_RAW_PATH}/pr.measure"))

@dlt.table(name="bronze_bls_duration", comment="Raw dimensional tracking frequencies from pr.duration")
def bronze_bls_duration():
    return clean_bronze_headers(spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","\t").load(f"{BLS_RAW_PATH}/pr.duration"))

@dlt.table(name="bronze_bls_period", comment="Raw dimensional calendar translations from pr.period")
def bronze_bls_period():
    return clean_bronze_headers(spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","\t").load(f"{BLS_RAW_PATH}/pr.period"))

@dlt.table(name="bronze_population", comment="Raw population records loaded from nested JSON", table_properties={"delta.columnMapping.mode": "name"})
def bronze_population():
    return spark.read.format("json").option("multiLine", "true").load(POP_RAW_PATH)

# ==============================================================================
# 2. SILVER LAYER (Data Quality Enforcement & Clear Relational Structuring)
# ==============================================================================

@dlt.table(name="silver_bls_data")
@dlt.expect_or_drop("valid_bls_keys", "series_id IS NOT NULL AND year IS NOT NULL")
def silver_bls_data():
    return dlt.read("bronze_bls_data").select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.col("year").cast("int").alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.col("value").cast("double").alias("value")
    )

@dlt.table(name="silver_bls_series")
@dlt.expect_or_drop("valid_series_keys", "series_id IS NOT NULL")
def silver_bls_series():
    return dlt.read("bronze_bls_series").select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.trim(F.col("sector_code")).alias("sector_code"),
        F.trim(F.col("measure_code")).alias("measure_code"),
        F.trim(F.col("duration_code")).alias("duration_code"),
        #F.trim(F.col("series_title")).alias("series_title")
    )

@dlt.table(name="silver_bls_sector")
def silver_bls_sector():
    return dlt.read("bronze_bls_sector").select(F.trim(F.col("sector_code")).alias("sector_code"), F.trim(F.col("sector_name")).alias("sector_name"))

@dlt.table(name="silver_bls_measure")
def silver_bls_measure():
    return dlt.read("bronze_bls_measure").select(F.trim(F.col("measure_code")).alias("measure_code"), F.trim(F.col("measure_text")).alias("measure_name"))

@dlt.table(name="silver_population")
@dlt.expect_or_drop("valid_population_value", "population > 0")
def silver_population():
    exploded_df = dlt.read("bronze_population").select(F.explode(F.col("data")).alias("record"))
    return exploded_df.select(
        F.col("record.Year").cast("int").alias("year"),
        F.col("record.Nation").cast("string").alias("nation"),
        F.col("record.Population").cast("long").alias("population")
    )

# ------------------------------------------------------------------------------
# THE IMPRESSIVE MOVE: Dynamic Dimensional Enrichment Lookups
# ------------------------------------------------------------------------------
@dlt.table(
    name="silver_enriched_bls_metadata",
    comment="SILVER DIMENSION: Unified metadata table resolving cryptic codes to clear human descriptions."
)
def silver_enriched_bls_metadata():
    return (
        dlt.read("silver_bls_series").alias("s")
        .join(dlt.read("silver_bls_sector").alias("sec"), on="sector_code", how="left")
        .join(dlt.read("silver_bls_measure").alias("m"), on="measure_code", how="left")
        .select(
            F.col("s.series_id"),
            #F.col("s.series_title"),
            F.col("sec.sector_name"),
            F.col("m.measure_name")
        )
    )

# ==============================================================================
# 3. GOLD LAYER - ANALYTICAL MASTER DATA ASSETS
# ==============================================================================

# Question 1: Annual US Population Stats (2013-2018 Inclusive)
@dlt.table(name="gold_us_population_stats")
def gold_us_population_stats():
    return dlt.read("silver_population").filter((F.col("year") >= 2013) & (F.col("year") <= 2018)).select(F.avg("population").alias("mean_population"), F.stddev("population").alias("stddev_population"))

# Question 2: Top Productivity Years by Series with Unified Dimensional Text Labels
@dlt.table(name="gold_top_productivity_years")
def gold_top_productivity_years():
    annual_sums = dlt.read("silver_bls_data").groupBy("series_id", "year").agg(F.sum("value").alias("total_annual_value"))
    window_spec = Window.partitionBy("series_id").orderBy(F.col("total_annual_value").desc())
    ranked_years = annual_sums.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1).drop("rank")
    
    # Joins against our new unified silver metadata master view!
    return ranked_years.join(dlt.read("silver_enriched_bls_metadata"), on="series_id", how="inner")

# Question 3: Generic Master Combination (Unbounded Production Table)
@dlt.table(name="gold_productivity_vs_population_master")
def gold_productivity_vs_population_master():
    return (
        dlt.read("silver_bls_data").select("year", "series_id", "period", F.col("value").alias("productivity_value"))
        .join(dlt.read("silver_population"), on="year", how="left")
    )

@dlt.table(
    name="gold_sector_benchmarking_summary",
    comment="VALUE-ADD GOLD: Aggregates historical productivity performance grouped cleanly by industry macro sectors."
)
def gold_sector_benchmarking_summary():
    # Join fact metrics with our wide dimension lookup metadata
    fact_with_metadata = (
        dlt.read("silver_bls_data")
        .join(dlt.read("silver_enriched_bls_metadata"), on="series_id", how="inner")
    )
    
    # Provide high-level macroeconomic summaries grouped by Sector name
    return (
        fact_with_metadata
        .groupBy("sector_name", "year")
        .agg(
            F.avg("value").alias("average_sector_productivity"),
            F.max("value").alias("peak_sector_productivity_value"),
            F.min("value").alias("floor_sector_productivity_value")
        )
        .orderBy("sector_name", "year")
    )

@dlt.table(
    name="gold_productivity_quarterly_trends",
    comment="VALUE-ADD GOLD: Calculates Quarter-over-Quarter (QoQ) percentage shifts in productivity values across series."
)
def gold_productivity_quarterly_trends():
    window_prev_quarter = Window.partitionBy("series_id", "year").orderBy("period")
    
    return (
        dlt.read("silver_bls_data")
        .withColumn("previous_quarter_value", F.lag("value", 1).over(window_prev_quarter))
        .withColumn("qoq_growth_rate", 
            F.when(F.col("previous_quarter_value").isNotNull(), 
                   ((F.col("value") - F.col("previous_quarter_value")) / F.col("previous_quarter_value")) * 100)
            .otherwise(F.lit(0))
        )
    )
    
# COMMAND ----------

# ==============================================================================
# 4. REQUIRED ALTERNATIVE IMPLEMENTATIONS (SPARK SQL TIER)
# ==============================================================================

# ------------------------------------------------------------------------------
# Question 1 Alternative: Spark SQL Implementation
# ------------------------------------------------------------------------------
@dlt.table(
    name="gold_us_population_stats_sql",
    comment="ALTERNATIVE SPARK SQL: Mean and Standard Deviation of US Population (2013-2018)."
)
def gold_us_population_stats_sql():
    # Executes declarative SQL strings against the live Silver layer view names
    return spark.sql("""
        SELECT 
            AVG(population) AS mean_population,
            STDDEV(population) AS stddev_population
        FROM live.silver_population
        WHERE year BETWEEN 2013 AND 2018
    """)


# ------------------------------------------------------------------------------
# Question 2 Alternative: Spark SQL Implementation
# ------------------------------------------------------------------------------
@dlt.table(
    name="gold_top_productivity_years_sql",
    comment="ALTERNATIVE SPARK SQL: Highest performing productivity years per unique series_id."
)
def gold_top_productivity_years_sql():
    return spark.sql("""
        WITH annual_sums AS (
            SELECT series_id, year, SUM(value) AS total_annual_value
            FROM live.silver_bls_data
            GROUP BY series_id, year
        ),
        ranked_years AS (
            SELECT series_id, year, total_annual_value,
                   ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_annual_value DESC) as rank
            FROM annual_sums
        )
        SELECT r.series_id, r.year, r.total_annual_value
        FROM ranked_years r
        INNER JOIN live.silver_bls_series s ON r.series_id = s.series_id
        WHERE r.rank = 1
    """)


# ------------------------------------------------------------------------------
# Question 3 Alternative: Spark SQL Implementation
# ------------------------------------------------------------------------------
@dlt.table(
    name="gold_productivity_vs_population_master_sql",
    comment="ALTERNATIVE SPARK SQL: Master dataset combining all BLS Series metrics with historical US Population data."
)
def gold_productivity_vs_population_master_sql():
    return spark.sql("""
        SELECT 
            b.year,
            b.series_id,
            b.period,
            b.value AS productivity_value,
            p.population
        FROM live.silver_bls_data b
        LEFT JOIN live.silver_population p ON b.year = p.year
    """)


