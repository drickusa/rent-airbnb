# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # 001 - Bronze Ingest
# MAGIC
# MAGIC This notebook ingests raw data files from the workspace into the **bronze** layer of the `sandbox_general` catalog.
# MAGIC
# MAGIC The databricks CATALOG and schema's are created as well if it is not existing yet in the databricks workspace
# MAGIC
# MAGIC **Sources:**
# MAGIC * `airbnb.csv` — Airbnb listing data
# MAGIC * `rentals.json` — Rental listing data
# MAGIC
# MAGIC **Targets:**
# MAGIC * `sandbox_general.bronze.airbnb`
# MAGIC * `sandbox_general.bronze.rentals`
# MAGIC
# MAGIC Files are read via pandas (workspace path workaround for serverless) and written as managed Delta tables using overwrite mode.

# COMMAND ----------

# DBTITLE 1,Initialize spark session
#set all variables to be used in this notebook
catalog_name = "sandbox_general"
schema_name_bronze = "bronze"
schema_name_silver = "silver"
schema_name_gold = "gold"
airbnb_input_csv_path = "/Workspace/Users/drickus.annandale@bngbank.nl/rent-airbnb/data/input/airbnb.csv"
rentals_input_csv_path = "/Workspace/Users/drickus.annandale@bngbank.nl/rent-airbnb/data/input/rentals.json"

airbnb_table_name = "airbnb"
rentals_table_name = "rentals"

airbnb_table_name_full = f"{catalog_name}.{schema_name_bronze}.{airbnb_table_name}"
rentals_table_name_full = f"{catalog_name}.{schema_name_bronze}.{rentals_table_name}"


# COMMAND ----------

# DBTITLE 1,Create Catalog and Schema's
#park.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name_bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name_silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name_gold}")

# COMMAND ----------

# DBTITLE 1,Ingest datasets
import pandas as pd
from pyspark.sql import functions as F

#ingest airnbnb dataset using databricks managed table 
# had to use panda ingestion as a workaround to read files form the workspace and to not load files to the volumes
pandas_airbnb = pd.read_csv(airbnb_input_csv_path)
df_airbnb = spark.createDataFrame(pandas_airbnb).withColumn("ingestedDateTime", F.current_timestamp())
df_airbnb.write.mode("overwrite").saveAsTable(airbnb_table_name_full)

#create rentals dataset schema before ingesting
dtypes_dict = {
    '_id': object,  # Array - keep as object/list
    'additionalCostsRaw': str,
    'areaSqm': str,
    'city': str,
    'crawlStatus': str,
    'crawledAt': object,  # Array - keep as object/list
    'deposit': str,
    'descriptionTranslated': str,
    'detailsCrawledAt': object,  # Array - keep as object/list
    'energyLabel': str,
    'firstSeenAt': object,  # Array - keep as object/list
    'furnish': str,
    'gender': str,
    'internet': str,
    'isRoomActive': str,  # Will convert to bool separately
    'kitchen': str,
    'lastSeenAt': object,  # Array - keep as object/list
    'latitude': float,
    'living': str,
    'longitude': float,
    'matchCapacity': str,
    'pageDescription': str,
    'pageTitle': str,
    'pets': str,
    'postalCode': str,
    'postedAgo': str,
    'propertyType': str,
    'availability': str,
    'registrationCost': str,
    'rent': str,
    'roommates': 'Int64',  # Nullable integer
    'shower': str,
    'smokingInside': str,
    'source': str,
    'title': str,
    'toilet': str
}

#ingest rentals dataset using databricks managed table 
pandas_rentals = pd.read_json(rentals_input_csv_path, dtype=dtypes_dict)
df_rentals = spark.createDataFrame(pandas_rentals).withColumn("ingestedDateTime", F.current_timestamp())
df_rentals.write.mode("overwrite").saveAsTable(rentals_table_name_full)