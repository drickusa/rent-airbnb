# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # 003 - Silver Load Rentals
# MAGIC
# MAGIC This notebook transforms the bronze rentals data into the **silver** layer with cleaning and standardization.
# MAGIC
# MAGIC **Transformations applied:**
# MAGIC * Remove duplicate records
# MAGIC * Clean currency columns (`additionalCosts`, `deposit`, `registrationCost`) — strip symbols, handle nulls/NA/None
# MAGIC * Extract `rentAmount` as integer from free-text rent field
# MAGIC * Flag whether utilities are included (`rentAmountUtilitiesIncluded`)
# MAGIC * Parse `availability` into `availabilityStartDate` / `availabilityEndDate`
# MAGIC * Validate and normalize Dutch postal codes (format: `1234 AB`)
# MAGIC * Generate `listingPk` via MD5 hash of lat/long coordinates
# MAGIC * Add `insertedDatetime` and `updatedDatetime` audit columns
# MAGIC * Rename columns to camelCase with descriptive names
# MAGIC * Drop obsolete crawl metadata columns
# MAGIC
# MAGIC **Source:** `sandbox_general.bronze.rentals`
# MAGIC **Target:** `sandbox_general.silver.rentals`

# COMMAND ----------

# DBTITLE 1,Read From Source
from pyspark.sql import functions as F

#set all variables to be used in this notebook
bronze_table_name = f"sandbox_general.bronze.rentals"
silver_table_name = f"sandbox_general.silver.rentals"

df = spark.read.format("delta").table(bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Read From Source
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import col

bronze_table_name = f"sandbox_general.bronze.rentals"
silver_table_name = f"sandbox_general.silver.rentals"

df = spark.read.format("delta").table(bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Clean and transform rentals Dataset
#First remove duplicates 
df = df.dropDuplicates()

valid_zip_pattern = r"^\d{4}\s*[A-Za-z]{2}$"

#clean dataset and split columns into more than one column to get more value for the data
df_cleaned = df.withColumn("_id", F.col("_id")[0]) \
               .withColumn("additionalCostsRaw",
                                F.when(
                                    F.col("additionalCostsRaw").isNull()
                                    | (F.upper(F.trim(F.col("additionalCostsRaw"))) == "NA")
                                    | (F.upper(F.trim(F.col("additionalCostsRaw"))) == "NONE")
                                    | (F.regexp_replace(F.col("additionalCostsRaw").cast("string"), r"[\s\u00A0]", "") == "-"),
                                    F.lit("0").cast("int")
                                ).otherwise(
                                    F.regexp_replace(
                                        F.regexp_replace(F.col("additionalCostsRaw").cast("string"), "€", ""),
                                        r"[\s\u00A0]",
                                        ""
                                    ).cast("int")
                                )
                            ) \
               .withColumn("areaSqm", F.regexp_replace(col("areaSqm"), " m2", "")) \
               .withColumn("deposit",F.when(
                                            F.col("deposit").isNull()
                                            | (F.upper(F.trim(F.col("deposit").cast("string"))) == "NA")
                                            | (F.upper(F.trim(F.col("deposit").cast("string"))) == "0")
                                            | (F.upper(F.trim(F.col("deposit").cast("string"))) == "NONE")
                                            | (F.regexp_replace(F.col("deposit").cast("string"), r"[\s\u00A0]", "") == "-"),
                                            F.lit("0").cast("int")
                                        ).otherwise(
                                            F.regexp_replace(
                                                F.regexp_replace(F.col("deposit").cast("string"), "€", ""),
                                                r"[\s\u00A0]",
                                                ""
                                            ).cast("int")
                                        )) \
               .withColumn("crawledAt", F.col("crawledAt")[0])   \
               .withColumn("availabilityStartDate", F.to_date(F.regexp_replace(F.split(F.col("availability"), " - ")[0], "'", ""), "dd-MM-yy")) \
               .withColumn("availabilityEndDate", F.when(F.split(F.col("availability"), " - ")[1] == "Indefinite period", "9999-12-31")
                                                   .otherwise(F.to_date(F.regexp_replace(F.split(F.col("availability"), " - ")[1], "'", ""), "dd-MM-yy"))) \
               .withColumn("postalCode",F.when(F.col("postalCode").rlike(valid_zip_pattern),F.upper(
                                                    F.regexp_replace(
                                                        F.trim(F.col("postalCode")),
                                                        r"^(\d{4})\s*([A-Za-z]{2})$",
                                                        r"$1 $2"
                                                    )
                                               )).otherwise(F.col("postalCode"))) \
               .withColumn("registrationCost",F.when(
                                            F.col("registrationCost").isNull()
                                            | (F.upper(F.trim(F.col("registrationCost").cast("string"))) == "NA")
                                            | (F.upper(F.trim(F.col("registrationCost").cast("string"))) == "NONE")
                                            | (F.regexp_replace(F.col("registrationCost").cast("string"), r"[\s\u00A0]", "") == "-"),
                                            F.lit("0").cast("int")
                                        ).otherwise(
                                            F.regexp_replace(
                                                F.regexp_replace(F.col("registrationCost").cast("string"), "€", ""),
                                                r"[\s\u00A0]",
                                                ""
                                            ).cast("int")
                                        )) \
               .withColumn("rentAmount",F.regexp_replace(F.regexp_extract(F.col("rent"), r"(\d[\d.,]*)", 1), r"[.,]-?$", "").cast("int")) \
               .withColumn("rentAmountUtilitiesIncluded",F.when(F.col("rent").rlike(r"Utilities incl\."),
                                                                F.lit("yes")
                                                            ).otherwise(F.lit("no"))
                                                     ) \
               .withColumn("listingPk",
                                    F.md5(
                                        F.concat_ws(
                                            ",",
                                            F.col("latitude").cast("string"),
                                            F.col("longitude").cast("string")
                                        ))) \
               .withColumn("insertedDatetime", F.current_timestamp()) \
               .withColumn("updatedDatetime", F.lit(None).cast("timestamp"))

#remove columms which are not needed 
df_cleaned = df_cleaned.drop("crawlStatus","detailsCrawledAt","firstSeenAt","lastSeenAt","availability","rent","crawledAt")

#rename columns to standard naming convention and use descriptive names 
df_final = df_cleaned.withColumnRenamed("_id", "rentalId") \
                     .withColumnRenamed("descriptionTranslated", "propertyDescription") \
                     .withColumnRenamed("pageDescription", "webpageDescription") \
                     .withColumnRenamed("pageTitle", "webpageTitle") \
                     .withColumnRenamed("additionalCostsRaw","additionalCosts")

#final select of the columns
df_final = df_final.select(
    "listingPk",
    "rentalId",
    "rentAmount",
    "rentAmountUtilitiesIncluded",
    "registrationCost",
    "additionalCosts",
    *[
        c for c in df_final.columns
        if c not in {
            "rentalId",
            "listingPk",           
            "rentAmount",
            "rentAmountUtilitiesIncluded",
            "registrationCost",
            "additionalCosts"
        }
    ]
)
#display(df_final)

# COMMAND ----------

# DBTITLE 1,Persist silver layer
#persist dataset to disk with overwrite mode for now - ToDo - create SCD Type 1 structure to only insert new records and update existing
df_final.write.mode("overwrite").saveAsTable(silver_table_name)