# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Products
# MAGIC
# MAGIC ## Contract
# MAGIC - One row per product (`product_id`)
# MAGIC - No null business keys (`product_id`)
# MAGIC - Clean and standardised product attributes
# MAGIC - Valid physical dimensions and weight values
# MAGIC - Join-safe to `order_items_silver`
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# Bronze and Silver base paths
bronze_products_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/products/"
silver_products_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/products/"


# COMMAND ----------

keys = TABLE_KEYS["products"]


# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

products_df = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(bronze_products_path)
)


# COMMAND ----------

products_schema = {
    "product_id": "string",
    "product_category_name": "string",
    "product_name_length": "integer",
    "product_description_length": "integer",
    "product_photos_qty": "integer",
    "product_weight_g": "double",
    "product_length_cm": "double",
    "product_height_cm": "double",
    "product_width_cm": "double"
}

# COMMAND ----------

# Silver transformations applied in sequence
products_clean = (
    products_df
    .transform(standardize_column_names)
    .transform(trim_string_columns)
    .transform(lambda df: enforce_schema(df, products_schema))
    .transform(nullify_blanks)
    .transform(lambda df: df.filter(
        F.col("product_id").isNotNull()
    ))
    .transform(lambda df: df.filter(
        (F.col("product_weight_g").isNull() | (F.col("product_weight_g") >= 0)) &
        (F.col("product_length_cm").isNull() | (F.col("product_length_cm") >= 0)) &
        (F.col("product_height_cm").isNull() | (F.col("product_height_cm") >= 0)) &
        (F.col("product_width_cm").isNull() | (F.col("product_width_cm") >= 0))
    ))
    .transform(lambda df: remove_duplicates(df, keys))
    .transform(lambda df: add_metadata(df, "products","olist"))
)

# COMMAND ----------

dq_report = dq_check(products_clean, keys, "products")


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
    "Data quality check failed: Null product_id detected"

# COMMAND ----------

merge_to_silver(
    df=products_clean,
    table_name="silver.products",
    merge_condition="t.product_id = s.product_id"
)
print("Silver products table written successfully.")
