# Databricks notebook source
# MAGIC %md
# MAGIC #  Silver Layer — Reviews
# MAGIC
# MAGIC ## Contract
# MAGIC - One row per review (`review_id`)
# MAGIC - No null business keys (`review_id`, `order_id`)
# MAGIC - Valid review scores (1–5)
# MAGIC - Correctly typed review timestamps
# MAGIC - Join-safe to `orders_silver`
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

# Bronze and Silver base paths
bronze_order_reviews_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/order_reviews/"
silver_order_reviews_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/order_reviews/"


# COMMAND ----------

keys = TABLE_KEYS["order_reviews"]


# COMMAND ----------

configure_adls_auth()

# COMMAND ----------

order_reviews_df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .option("quote", "\"")        
        .option("escape", "\"")     
        .option("multiLine", True)    
        .option("mode", "PERMISSIVE") 
        .load(bronze_order_reviews_path)
)


# COMMAND ----------

order_reviews_schema = {
    "review_id": "string",
    "order_id": "string",
    "review_score": "integer",
    "review_comment_title": "string",
    "review_comment_message": "string",
    "review_creation_date": "timestamp",
    "review_answer_timestamp": "timestamp"
    }


# COMMAND ----------

# Silver transformation
order_reviews_clean = (
    order_reviews_df
    .transform(standardize_column_names)
    .transform(trim_string_columns)
    .transform(lambda df: enforce_schema(df, order_reviews_schema))
    .transform(nullify_blanks)
    .transform(lambda df: df.filter(
        F.col("review_id").isNotNull() &
        F.col("order_id").isNotNull()
    ))
    .transform(lambda df: df.filter(
        (F.col("review_score") >= 1) &
        (F.col("review_score") <= 5)
    ))
    .transform(lambda df: remove_duplicates(df,keys))
    .transform(lambda df: add_metadata(df, "order_reviews","olist"))
)

# COMMAND ----------

dq_report = dq_check(order_reviews_clean, keys, "order_reviews")

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
    "Data quality check failed: Null review_id detected"


# COMMAND ----------

merge_to_silver(
    df=order_reviews_clean,
    table_name="silver.reviews",
    merge_condition="t.review_id = s.review_id"
)
print("Silver order_reviews table written successfully.")

