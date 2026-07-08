# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # 999 - Pipeline Runner
# MAGIC
# MAGIC Orchestrates the full ETL pipeline by executing notebooks 001–004 sequentially:
# MAGIC 1. **Bronze** — Ingest raw CSV/JSON into bronze tables
# MAGIC 2. **Silver** — Clean and standardize Airbnb & rental data
# MAGIC 3. **Gold** — Aggregate revenue per listing and postal code

# COMMAND ----------

# DBTITLE 1,Run pipeline notebooks sequentially
notebooks = [
    "001_bronze_ingest",
    "002_silver_load_airbnb",
    "003_silver_load_rentals",
    "004_gold_load_revenue",
]

for notebook in notebooks:
    print(f"Running {notebook}...")
    dbutils.notebook.run(f"/Users/drickus.annandale@bngbank.nl/rent-airbnb/notebooks/{notebook}", timeout_seconds=600)
    print(f"✓ {notebook} completed")

print("\nAll notebooks executed successfully.")