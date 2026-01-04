# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Order Items
# MAGIC
# MAGIC ## Contract
# MAGIC - One row per order line (`order_id`, `order_item_id`)
# MAGIC - No null business keys (`order_id`, `order_item_id`, `product_id`, `seller_id`)
# MAGIC - Correct numeric types for price and freight values
# MAGIC - Join-safe to `orders_silver`, `products_silver`, and `sellers_silver`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# Bronze and Silver base paths
bronze_order_items_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/order_items/"
silver_order_items_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/order_items/"


# COMMAND ----------

keys = TABLE_KEYS["order_items"]


# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

order_items_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(bronze_order_items_path)
)


# COMMAND ----------

order_items_schema = {
    "order_id": "string",
    "order_item_id": "int",
    "product_id": "string",
    "seller_id": "string",
    "shipping_limit_date": "timestamp",
    "price": "decimal(10,2)",
    "freight_value": "decimal(10,2)"
}



# COMMAND ----------

# Silver transformations applied in sequence
order_items_clean = (
    order_items_df
    .transform(standardize_column_names)
    .transform(trim_string_columns)
    .transform(lambda df: enforce_schema(df, order_items_schema))
    .transform(nullify_blanks)
    .transform(lambda df: df.filter(
        F.col("order_id").isNotNull() &
        F.col("order_item_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("seller_id").isNotNull()
    ))
    .transform(lambda df: df.filter(
        (F.col("price") >= 0) &
        (F.col("freight_value") >= 0)
    ))
    .transform(lambda df: df.filter(
        F.col("shipping_limit_date").isNotNull()
    ))
    .transform(lambda df: remove_duplicates(
        df, keys 
    ))

    .transform(lambda df: add_metadata(df, "order_items","olist"))
)

# COMMAND ----------


dq_report = dq_check(
    order_items_clean,
    keys , 
    "order_items"
)

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
    "Data quality check failed: Null order_items primary key detected"


# COMMAND ----------

merge_to_silver(
    df=order_items_clean,
    table_name="silver.order_items",
    merge_condition="""
        t.order_id = s.order_id
        AND t.order_item_id = s.order_item_id
    """
)
print("Silver order_items table written successfully.")
