# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Runner
# MAGIC
# MAGIC ## Purpose
# MAGIC - Orchestrates execution of all Silver-layer transformations
# MAGIC - Ensures correct dependency order
# MAGIC - Fails fast on data quality violations
# MAGIC
# MAGIC ## Execution
# MAGIC - Triggered by Azure Data Factory
# MAGIC - Must complete successfully before Gold layer runs
# MAGIC

# COMMAND ----------

# MAGIC %run "../00_setup/00_config_setup"

# COMMAND ----------

# MAGIC %run "../00_setup/00_common_functions"

# COMMAND ----------

from pyspark.sql import functions as F

configure_adls_auth()

# COMMAND ----------

# MAGIC %run "./customers_silver"
# MAGIC

# COMMAND ----------

# MAGIC %run "./sellers_silver"
# MAGIC

# COMMAND ----------

# MAGIC %run "./products_silver"
# MAGIC

# COMMAND ----------

# MAGIC %run "./geolocation_silver"
# MAGIC

# COMMAND ----------

# MAGIC %run "./orders_silver"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "./order_items_silver"
# MAGIC

# COMMAND ----------

# MAGIC %run "./order_payments_silver"
# MAGIC

# COMMAND ----------

# MAGIC %run "./reviews_silver"
# MAGIC

# COMMAND ----------

print("✅ Silver layer completed successfully")