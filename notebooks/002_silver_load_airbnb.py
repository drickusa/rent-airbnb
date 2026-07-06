# Databricks notebook source
# DBTITLE 1,Read From Source
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F

#spark = SparkSession.builder.getOrCreate()

bronze_table_name = f"sandbox_general.bronze.airbnb"
silver_table_name = f"sandbox_general.silver.airbnb"

df = spark.read.format("delta").table(bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Clean AirBnb Dataset
#First remove duplicates and all ZIP Code = null 
df_airbnb = df.dropDuplicates().filter(df.zipcode.isNotNull())

#Correct the zip code with regular expression
#Matches: 1234AB or 1234 AB (and normalizes to 1234 AB)
valid_zip_pattern = r"^\d{4}\s*[A-Za-z]{2}$"

# do a replace of the current zipcode column with the enriched zipcode
df_airbnb = df_airbnb.withColumn("zipcode",F.when(F.col("zipcode").rlike(valid_zip_pattern),
                                                F.upper(
                                                    F.regexp_replace(
                                                        F.trim(F.col("zipcode")),
                                                        r"^(\d{4})\s*([A-Za-z]{2})$",
                                                        r"$1 $2"
                                                    )
                                                )
                                ).otherwise(F.col("zipcode"))) \
                     .withColumn("airbnb_pk",F.md5(F.concat_ws(",",
                                    F.col("latitude").cast("string"),
                                    F.col("longitude").cast("string")
                                ))) \
                     .withColumn("inserted_datetime",F.current_timestamp()) \
                     .select("airbnb_pk", *[c for c in df.columns])

df_airbnb.show()

# COMMAND ----------

# DBTITLE 1,Persist silver layer

#persist dataset to disk with overwrite mode for now - ToDo - create SCD Type 1 structure to only insert new records and update existing
df_airbnb.write.mode("overwrite").saveAsTable(silver_table_name)