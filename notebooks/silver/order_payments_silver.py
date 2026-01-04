# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Order Payments
# MAGIC
# MAGIC ## Contract
# MAGIC
# MAGIC This table represents payment events associated with customer orders.
# MAGIC
# MAGIC **Guarantees:**
# MAGIC - One row per payment event (`order_id`, `payment_sequential`)
# MAGIC - No null business keys (`order_id`, `payment_sequential`)
# MAGIC - Validated payment values and installment counts
# MAGIC - Standardised payment types
# MAGIC - Join-safe to `orders_silver`

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# Bronze and Silver base paths
bronze_order_payments_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/order_payments/"
silver_order_payments_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/order_payments/"


# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

keys = TABLE_KEYS["order_payments"]


# COMMAND ----------

order_payments_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(bronze_order_payments_path)
)


# COMMAND ----------

order_payments_schema = {
    "order_id": "string",
    "payment_sequential": "int",
    "payment_type": "string",
    "payment_installments": "int",
    "payment_value": "decimal(10,2)"
}


# COMMAND ----------

# Silver transformations
order_payments_clean = (
    order_payments_df
    .transform(standardize_column_names)
    .transform(trim_string_columns)
    .transform(lambda df: enforce_schema(df, order_payments_schema))
    .transform(nullify_blanks)
    .transform(lambda df: df.filter(
        F.col("order_id").isNotNull() &
        F.col("payment_sequential").isNotNull()
    ))
    .transform(lambda df: df.withColumn(
        "payment_type", F.lower(F.col("payment_type"))
    ))
    .transform(lambda df: df.filter(F.col("payment_value") >= 0))
    .transform(lambda df: df.filter(
        F.col("payment_installments").isNull() |
        (F.col("payment_installments") >= 1)
    ))
    .transform(lambda df: remove_duplicates(
        df, keys
    ))
    .transform(lambda df: add_metadata(df, "order_payments","olist"))
)

# COMMAND ----------

dq_report = dq_check(order_payments_clean, keys, "order_payments")

# COMMAND ----------

null_pk_rows = (
    dq_report
    .filter(F.col("metric") == "null_pk")
    .select("value")
    .collect()
)

if not null_pk_rows:
    raise ValueError("DQ check failed: metric 'null_pk' not found")

assert int(null_pk_rows[0][0]) == 0, \
    "Data quality check failed: Null order_payments primary key detected"
 

# COMMAND ----------

merge_to_silver(
    df=order_payments_clean,
    table_name="silver.order_payments",
    merge_condition="t.order_id = s.order_id AND t.payment_sequential = s.payment_sequential"
)

print("Silver order_payments table written successfully.")

