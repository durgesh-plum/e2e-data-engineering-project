# Databricks notebook source
# MAGIC %md
# MAGIC #  Silver Layer — Geolocation
# MAGIC
# MAGIC ## Contract
# MAGIC - One row per geolocation record (`geo_id`)
# MAGIC - No null business keys (`geo_id`)
# MAGIC - Clean and standardised location attributes
# MAGIC - Valid latitude and longitude values
# MAGIC - Join-safe to `customers_silver` and `sellers_silver`
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# Bronze and Silver base paths
bronze_geolocation_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/geolocation/"
silver_geolocation_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/geolocation/"


# COMMAND ----------

keys = TABLE_KEYS["geolocation"]


# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

geolocation_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(bronze_geolocation_path)
)

# COMMAND ----------

geolocation_schema = {
    "geolocation_zip_code_prefix": "string",
    "geolocation_lat": "double",
    "geolocation_lng": "double",
    "geolocation_city": "string",
    "geolocation_state": "string"
}

# COMMAND ----------

geolocation_clean = (
    geolocation_df
        .transform(standardize_column_names)
        .transform(trim_string_columns)
        .transform(lambda df: enforce_schema(df, geolocation_schema))
        .transform(nullify_blanks)
        .transform(lambda df: df.filter(
    (F.col("geolocation_lat").isNull() | F.col("geolocation_lat").between(-90, 90)) &
    (F.col("geolocation_lng").isNull() | F.col("geolocation_lng").between(-180, 180))
))
        .transform(lambda df: df.withColumn("geo_id", F.monotonically_increasing_id()))
        .transform(lambda df: add_metadata(df, "geolocation","olist"))
)


# COMMAND ----------

dq_report = dq_check(geolocation_clean, keys, "geolocation")

# COMMAND ----------

from pyspark.sql import functions as F

null_pk_rows = (
    dq_report
    .filter(F.col("metric") == "null_pk")
    .select("value")
    .collect()
)

if not null_pk_rows:
    raise ValueError("DQ check failed: metric 'null_pk' not found")

null_pk_count = int(null_pk_rows[0][0])

assert null_pk_count == 0, \
    "Data quality check failed: Null geo_id detected"


# COMMAND ----------

(
    geolocation_clean
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver.geolcation")
)

print("Silver geolocation table written successfully.")
