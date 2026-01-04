# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Orders
# MAGIC
# MAGIC ## Contract
# MAGIC - One row per order (`order_id`)
# MAGIC - No null business keys (`order_id`, `customer_id`)
# MAGIC - Correctly typed timestamps
# MAGIC - Clean and standardised order status values
# MAGIC - Join-safe to `customers_silver` and `order_items_silver`
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# Bronze and Silver base paths
bronze_orders_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/orders/"
silver_orders_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/orders/"


# COMMAND ----------

keys = TABLE_KEYS["orders"]


# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

orders_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(bronze_orders_path)
)

# COMMAND ----------

orders_schema = {
    "order_id": "string",
    "customer_id": "string",
    "order_status": "string",
    "order_purchase_timestamp": "timestamp",
    "order_approved_at": "timestamp",
    "order_delivered_carrier_date": "timestamp",
    "order_delivered_customer_date": "timestamp",
    "order_estimated_delivery_date": "timestamp"
}



# COMMAND ----------

# Silver transformations applied in sequence
orders_clean = (
    orders_df
    .transform(standardize_column_names)
    .transform(trim_string_columns)
    .transform(lambda df: enforce_schema(df, orders_schema))
    .transform(nullify_blanks)
    .transform(lambda df: df.filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull()
    ))
    .transform(lambda df: df.withColumn(
        "order_status", F.lower(F.col("order_status"))
    ))
    .transform(lambda df: df.filter(
        F.col("order_purchase_timestamp").isNotNull()
    ))
    .transform(lambda df: remove_duplicates(df, keys))
    .transform(lambda df: add_metadata(df, "orders", "olist"))
    )



# COMMAND ----------

dq_report = dq_check(orders_clean, keys, "orders")


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
    "Data quality check failed: Null order_id detected"


# COMMAND ----------

merge_to_silver(
    df=orders_clean,
    table_name="silver.order",
    merge_condition="t.order_id = s.order_id"
)
print("Silver orders table written successfully.")
