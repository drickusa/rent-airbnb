# Databricks notebook source
# DBTITLE 1,Read From Source
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import col
#spark = SparkSession.builder.getOrCreate()

bronze_table_name = f"sandbox_general.bronze.rentals"
silver_table_name = f"sandbox_general.silver.rentals"

df = spark.read.format("delta").table(bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Clean rentals Dataset
#First remove duplicates 
df_rentals = df.dropDuplicates()

valid_zip_pattern = r"^\d{4}\s*[A-Za-z]{2}$"

#extract id from the array
df_cleaned = df.withColumn("rentalsId", F.col("_id")[0]).drop("_id") \
               .withColumn("additionalCostsRaw", F.when((F.col("additionalCostsRaw").isNull()) | (F.col("additionalCostsRaw") == "NA"), None).otherwise(F.regexp_replace(F.col("additionalCostsRaw"), "€", "")))\
               .withColumnRenamed("additionalCostsRaw","additionalCosts") \
               .withColumn("areaSqm", F.regexp_replace(col("areaSqm"), " m2", "")) \
               .withColumn("deposit", F.when((F.col("deposit").isNull()) | (F.trim(F.col("deposit")) == "-"), None).otherwise(F.trim(F.regexp_replace(F.col("deposit"), "€", "")))) \
               .withColumnRenamed("descriptionTranslated","propertyDescription") \
               .withColumn("crawledAt", F.col("crawledAt")[0])   \
               .withColumn("availabilityStartDate", F.to_date(F.regexp_replace(F.split(F.col("availability"), " - ")[0], "'", ""), "dd-MM-yy")) \
               .withColumn("availabilityEndDate", F.when(F.split(F.col("availability"), " - ")[1] == "Indefinite period", "9999-12-31").otherwise(F.regexp_replace(F.split(F.col("availability"), " - ")[1], "'", ""))) \
               .withColumn("postalCode",F.when(F.col("postalCode").rlike(valid_zip_pattern),F.upper(
                                                    F.regexp_replace(
                                                        F.trim(F.col("postalCode")),
                                                        r"^(\d{4})\s*([A-Za-z]{2})$",
                                                        r"$1 $2"
                                                    )
                                                )
                                ).otherwise(F.col("postalCode")))


df_cleaned = df_cleaned.drop("crawlStatus","detailsCrawledAt","firstSeenAt","lastSeenAt","availability")

df_final = df_cleaned.select("rentalsId","crawledAt","postedAgo", *[c for c in df_cleaned.columns])
display(df_final)

# COMMAND ----------

# DBTITLE 1,Persist silver layer

#persist dataset to disk with overwrite mode for now - ToDo - create SCD Type 1 structure to only insert new records and update existing
df_rentals.write.mode("overwrite").saveAsTable(silver_table_name)