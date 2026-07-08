# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # 002 - Silver Load Airbnb
# MAGIC
# MAGIC This notebook transforms the bronze Airbnb data into the **silver** layer with cleaning and standardization.
# MAGIC
# MAGIC **Transformations applied:**
# MAGIC * Remove duplicate records
# MAGIC * Cast `latitude`, `longitude`, `bedrooms`, and `review_scores_value` to proper types
# MAGIC * Validate and normalize Dutch postal codes (format: `1234 AB`)
# MAGIC * Generate `listingPk` via MD5 hash of lat/long coordinates
# MAGIC * Add `source`, `insertedDatetime`, and `updatedDatetime` audit columns
# MAGIC * Rename columns to camelCase convention
# MAGIC
# MAGIC **Source:** `sandbox_general.bronze.airbnb`
# MAGIC **Target:** `sandbox_general.silver.airbnb`

# COMMAND ----------

# DBTITLE 1,Initialize
from pyspark.sql import functions as F

#set all variables to be used in this notebook
bronze_table_name = f"sandbox_general.bronze.airbnb"
silver_table_name = f"sandbox_general.silver.airbnb"

df = spark.read.format("delta").table(bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Clean and transform AirBnb Dataset
#First remove duplicates and all ZIP Code = null 
df_airbnb = df.dropDuplicates()

#regular expression to check and fix the postal code
valid_zip_pattern = r"^\d{4}\s*[A-Za-z]{2}$"

# do a replace of the current zipcode column with the enriched zipcode
df_cleaned = df_airbnb.withColumn("latitude", F.col("latitude").cast("double")) \
                      .withColumn("longitude", F.col("longitude").cast("double")) \
                      .withColumn("bedrooms", F.col("bedrooms").cast("int")) \
                      .withColumn("review_scores_value", F.col("review_scores_value").cast("int")) \
                      .withColumn("zipcode",
                                    F.when(
                                        F.col("zipcode").rlike(valid_zip_pattern),
                                        F.upper(
                                            F.regexp_replace(
                                                F.trim(F.col("zipcode")),
                                                r"^(\d{4})\s*([A-Za-z]{2})$",
                                                r"$1 $2"
                                            )
                                        )
                                    ).otherwise(F.col("zipcode"))
                                ) \
                      .withColumn("listingPk",
                                    F.md5(
                                        F.concat_ws(
                                            ",",
                                            F.col("latitude").cast("string"),
                                            F.col("longitude").cast("string")
                                        ))) \
                      .withColumn("source", F.lit("airbnb")) \
                      .withColumn("insertedDatetime", F.current_timestamp()) \
                      .withColumn("updatedDatetime", F.lit(None).cast("timestamp"))

#rename and final selection of columns
df_final = df_cleaned.withColumnRenamed("room_type", "roomType") \
                     .withColumnRenamed("review_scores_value", "reviewScoreValue") \
                     .withColumnRenamed("zipcode", "postalCode")

#select primary key in the first position               
df_final = df_final.select(
    "listingPk",
    *[c for c in df_final.columns if c != "listingPk"]
)
df_final.show()

# COMMAND ----------

# DBTITLE 1,Persist silver layer
#persist dataset to disk with overwrite mode for now - ToDo - create SCD Type 1 structure to only insert new records and update existing
df_final.write.mode("overwrite").saveAsTable(silver_table_name)