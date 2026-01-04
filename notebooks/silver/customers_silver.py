# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Customers
# MAGIC
# MAGIC ## Contract
# MAGIC - One row per unique customer (`customer_unique_id`)
# MAGIC - No null business keys (`customer_unique_id`)
# MAGIC - Clean and standardised customer attributes
# MAGIC - Valid Brazilian state codes
# MAGIC - Join-safe to `orders_silver`

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Bronze and Silver base paths
bronze_customers_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/customers/"
silver_customers_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/customers/"


# COMMAND ----------

keys = TABLE_KEYS["customers"]

# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

customers_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(bronze_customers_path)
)

# COMMAND ----------

customers_schema = {
    "customer_id": "string",
    "customer_unique_id": "string",
    "customer_zip_code_prefix": "string",
    "customer_city": "string",
    "customer_state": "string"
}


# COMMAND ----------

# customer_unique_id represents a unique individual, whereas customer_id can change across orders
customers_clean = (
    customers_df
    .transform(standardize_column_names)
    .transform(trim_string_columns)
    .transform(lambda df: enforce_schema(df, customers_schema))
    .transform(nullify_blanks)
    .transform(
        lambda df: df.filter(
            F.col("customer_unique_id").isNotNull() &
            F.col("customer_id").isNotNull()
        )
    )
    .withColumn(
        "customer_state",
        F.upper(F.trim(F.col("customer_state")))
    )
    .transform(
    lambda df: df.filter(F.length("customer_state") == 2)
)
    .transform(lambda df: remove_duplicates(df, keys))
    .transform(lambda df: add_metadata(df, "customers", "olist"))
)

# COMMAND ----------

dq_report = dq_check(customers_clean, keys, "customers")

# COMMAND ----------


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
    "Data quality check failed: Null customer_unique_id detected"


# COMMAND ----------

merge_to_silver(
    df=customers_clean,
    table_name="silver.customers",
    merge_condition="t.customer_unique_id = s.customer_unique_id"
)
print("Silver customers table written successfully.")
