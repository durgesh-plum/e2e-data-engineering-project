# Retail Sales Analytics Lakehouse 

Tech stack: Azure Data Factory, ADLS, Databricks (PySpark, Delta), Synapse, Power BI, Data Modelling

This project implements a modern lakehouse analytics platform on Azure using the Brazilian E-Commerce dataset by Olist.

Raw CSV data is ingested from cloud storage into a Bronze layer using Azure Data Factory.
Data is then cleaned, standardized, deduplicated, and quality-checked in Silver using Databricks.
Finally, analytics-ready Gold fact and dimension tables are produced and exposed to Power BI via a Databricks SQL Warehouse.

The pipeline is fully orchestrated, repeatable, and production-aligned.
