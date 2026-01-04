# Databricks notebook source
# Silver Notebook: Authentication Setup
scope_name = "olist-kv-scope"
tenant_id = dbutils.secrets.get(scope_name, "tenant-id")
client_id = dbutils.secrets.get(scope_name, "client-id")
client_secret = dbutils.secrets.get(scope_name, "client-secret")


# COMMAND ----------

storage_account_name = "stolistlakehousene"
container_name = "olist"
endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

# COMMAND ----------

# Apply Spark Configs to the CURRENT session
def configure_adls_auth():
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", 
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") # Verified with '2'

    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", endpoint)

    # Testing the connection 
    try:
        dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/bronze/")
        print("Authentication successful in Silver session.")
    except Exception as e:
        print(f"Authentication failed: {e}")

# COMMAND ----------

# =========================
# SILVER PATHS
# =========================

silver_customers_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/customers"
)

silver_sellers_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/sellers"
)

silver_orders_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/orders"
)

silver_order_items_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/order_items"
)

silver_order_payments_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/order_payments"
)

silver_reviews_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/order_reviews"
)

silver_products_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/products"
)

silver_geolocation_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/geolocation"
)


# COMMAND ----------

# =========================
# GOLD PATHS
# =========================

gold_base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold"

#dimensions
gold_dim_date_path = f"{gold_base_path}/dim_date"
gold_dim_geolocation_path = f"{gold_base_path}/dim_geolocation"
gold_dim_customers_path = f"{gold_base_path}/dim_customers"
gold_dim_products_path = f"{gold_base_path}/dim_products"
gold_dim_sellers_path = f"{gold_base_path}/dim_sellers"

#facts
gold_fact_orders_path = f"{gold_base_path}/fact_orders"
gold_fact_order_items_path = f"{gold_base_path}/fact_order_items"
