# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType

# COMMAND ----------

def standardize_column_names(df: DataFrame) -> DataFrame:
    # Fix Dataset Column Typo
    rename_map = {
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    }

    for col in df.columns:
        new_col = col.lower().strip().replace(" ", "_")
        new_col = rename_map.get(new_col, new_col)
        df = df.withColumnRenamed(col, new_col)
    return df


def trim_string_columns(df: DataFrame) -> DataFrame:
    string_cols = [c for c, t in df.dtypes if t == "string"]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))
    return df


def enforce_schema(df: DataFrame, schema: dict) -> DataFrame:
    for col, dtype in schema.items():
        if dtype == "timestamp":
            df = df.withColumn(col, F.to_timestamp(F.col(col), "yyyy-MM-dd HH:mm:ss"))
        elif dtype == "integer":
            df = df.withColumn(col, F.col(col).cast(IntegerType()))
        elif dtype == "double":
            df = df.withColumn(col, F.col(col).cast(DoubleType()))
        else:
            df = df.withColumn(col, F.col(col).cast(dtype))
    return df

 
def nullify_blanks(df):
    string_cols = [c for c, t in df.dtypes if t == "string" and c != ""]    
    for col in string_cols:
        df = df.withColumn(
            col, 
            F.when(F.trim(F.col(col)) == "", None).otherwise(F.col(col))
        )
    return df


def remove_duplicates(df: DataFrame, pk_list: list) -> DataFrame:
    return df.dropDuplicates(pk_list)


def add_metadata(df: DataFrame, table_name: str, source_name: str) -> DataFrame:
    ts = F.current_timestamp()
    return (
        df
        .withColumn("_silver_load_ts", ts)
        .withColumn("_ingest_ts", ts)
        .withColumn("_silver_source", F.lit(table_name))
        .withColumn("_source", F.lit(source_name))
    )


# COMMAND ----------

def dq_check(df: DataFrame, pk_cols: list, table_name: str):
    total_rows = df.count()

    null_pk = df.filter(
        " OR ".join([f"{c} IS NULL" for c in pk_cols])
    ).count()

    duplicate_pk = total_rows - df.dropDuplicates(pk_cols).count()

    string_cols = [c for c, t in df.dtypes if t == "string"]
    blank_counts = {c: df.filter(F.trim(F.col(c)) == "").count() for c in string_cols}
    null_counts = {c: df.filter(F.col(c).isNull()).count() for c in string_cols}

    rows = [
        ("table", table_name),
        ("total_rows", total_rows),
        ("null_pk", null_pk),
        ("duplicate_pk", duplicate_pk),
    ]

    for c in string_cols:
        rows.append((f"null_{c}", null_counts[c]))
        rows.append((f"blank_{c}", blank_counts[c]))

    dq_report = spark.createDataFrame(rows, ["metric", "value"])
    
    return dq_report



# COMMAND ----------

from delta.tables import DeltaTable

def merge_to_silver(
    df,
    table_name,
    merge_condition
):
    if spark.catalog.tableExists(table_name):
        target = DeltaTable.forName(spark, table_name)

        (
            target.alias("t")
            .merge(
                df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )


# COMMAND ----------

TABLE_KEYS = {
    "customers": ["customer_unique_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"]
}
