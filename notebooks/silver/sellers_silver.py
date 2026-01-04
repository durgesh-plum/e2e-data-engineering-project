# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Sellers
# MAGIC
# MAGIC ## Contract
# MAGIC - One row per seller (`seller_id`)
# MAGIC - No null business keys (`seller_id`)
# MAGIC - Clean and standardised string fields
# MAGIC - Join-safe to `order_items_silver`
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# Bronze and Silver base paths
bronze_sellers_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/sellers/"
silver_sellers_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/sellers/"


# COMMAND ----------

keys = TABLE_KEYS["sellers"]


# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

sellers_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(bronze_sellers_path)
)

# COMMAND ----------

sellers_schema = {
    "seller_id": "string",
    "seller_zip_code_prefix": "string",
    "seller_city": "string",
    "seller_state": "string"
}


# COMMAND ----------

# Silver transformations
sellers_clean = (
    sellers_df
        .transform(standardize_column_names)
        .transform(trim_string_columns)
        .transform(lambda df: enforce_schema(df, sellers_schema))
        .transform(nullify_blanks)
        .transform(lambda df: df.filter(
        (F.col("seller_id").isNotNull()) &
        (F.length("seller_state") == 2)))
        .transform(lambda df: remove_duplicates(df, keys))
        .transform(lambda df: add_metadata(df, "sellers","olist"))
)


# COMMAND ----------

dq_report = dq_check(sellers_clean,keys, "sellers")

# COMMAND ----------


null_pk_rows = (
    dq_report
    .filter(F.col("metric") == "null_pk")
    .select("value")
    .collect()
)

if not null_pk_rows:
    raise ValueError(
        "DQ check failed: metric 'null_pk' not found in dq_report"
    )

null_pk_count = int(null_pk_rows[0][0])

assert null_pk_count == 0, \
    "Data quality check failed: Null seller_id detected"


# COMMAND ----------

merge_to_silver(
    df=sellers_clean,
    table_name="silver.sellers",
    merge_condition="t.seller_id = s.seller_id"
)
print("Silver sellers table written successfully.")

