# Databricks notebook source
# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # 004 - Gold Load Revenue
# MAGIC
# MAGIC This notebook aggregates silver-layer data into the **gold** layer, producing revenue summaries for analysis.
# MAGIC
# MAGIC **Outputs:**
# MAGIC * `sandbox_general.gold.airbnb_per_listing` — total revenue per Airbnb listing
# MAGIC * `sandbox_general.gold.airbnb_per_postalcode` — total revenue per postal code (Airbnb)
# MAGIC * `sandbox_general.gold.rental_per_listing` — total revenue per rental listing
# MAGIC * `sandbox_general.gold.rental_per_postalcode` — total revenue per postal code (rentals)
# MAGIC
# MAGIC **Sources:**
# MAGIC * `sandbox_general.silver.airbnb`
# MAGIC * `sandbox_general.silver.rentals`
# MAGIC
# MAGIC Each gold table includes audit columns (`insertDateTime`, `updatedDateTime`) and is written in overwrite mode.

# COMMAND ----------

# DBTITLE 1,Initialize
from pyspark.sql import functions as F

#set all variables to be used in this notebook
silver_rentals_table_name = f"sandbox_general.silver.rentals"
silver_airbnb_table_name = f"sandbox_general.silver.airbnb"
gold_airbnb_per_listing_table_name = f"sandbox_general.gold.airbnb_per_listing"
gold_airbnb_per_postalcode_table_name = f"sandbox_general.gold.airbnb_per_postalcode"
gold_rental_per_listing_table_name = f"sandbox_general.gold.rental_per_listing"
gold_rental_per_postalcode_table_name = f"sandbox_general.gold.rental_per_postalcode"

df_rental = spark.read.format("delta").table(silver_rentals_table_name)
df_airbnb = spark.read.format("delta").table(silver_airbnb_table_name)

# COMMAND ----------

# DBTITLE 1,transform revenue for airbnb_per_listing
df_airbnb_revenue_per_listing = (
     df_airbnb
    .groupBy("listingPk", "source", "latitude", "longitude")
    .agg(F.sum("price").alias("Revenue"))
    .select(
        "listingPk",
        "Revenue",
        F.col("source").alias("Source"),
        F.col("latitude").alias("Latitude"),
        F.col("longitude").alias("Longitude"),
        F.current_timestamp().alias("InsertedDateTime"),
        F.lit(None).cast("timestamp").alias("UpdatedDateTime"),
    )
)
#display(df_airbnb_revenue_per_listing)

# COMMAND ----------

# DBTITLE 1,transform revenue for airbnb_per_postalcode
df_airbnb_postalcode_revenue = (
    df_airbnb
    .groupBy("postalCode", "source")
    .agg(F.sum("price").alias("Revenue"))
    .select(
        F.col("postalCode").alias("PostalCode"),
        "Revenue",
        F.col("source").alias("Source"),
        F.current_timestamp().alias("InsertedDateTime"),
        F.lit(None).cast("timestamp").alias("UpdatedDateTime"),
    )
)
#display(df_airbnb_postalcode_revenue)

# COMMAND ----------

# DBTITLE 1,transform revenue for rentals per Listing
df_rental_revenue_per_listing = (
    df_rental
    .groupBy("listingPk", "source")
    .agg(F.sum("rentAmount").alias("Revenue"))
    .select(
        "listingPk",
        "Revenue",
        F.col("source").alias("Source"),
        F.current_timestamp().alias("InsertedDateTime"),
        F.lit(None).cast("timestamp").alias("UpdatedDateTime"),
    )
)

# COMMAND ----------

# DBTITLE 1,transform revenue for rentals per postalcode
df_rental_revenue_per_postalcode = (df_rental.groupBy("postalCode", "source")
                .agg(F.sum("rentAmount").alias("Revenue"))
                .select(
                    F.col("postalCode").alias("PostalCode"),
                    "Revenue",
                    F.col("source").alias("Source"),
                    F.current_timestamp().alias("InsertedDateTime"),
                    F.lit(None).cast("timestamp").alias("UpdatedDateTime"),)
     )

# COMMAND ----------

# DBTITLE 1,Persist all dataframes to managed delta tables with overwrite
df_airbnb_revenue_per_listing.write.mode("overwrite").saveAsTable(gold_airbnb_per_listing_table_name)
df_airbnb_postalcode_revenue.write.mode("overwrite").saveAsTable(gold_airbnb_per_postalcode_table_name)
df_rental_revenue_per_listing.write.mode("overwrite").saveAsTable(gold_rental_per_listing_table_name)
df_rental_revenue_per_postalcode.write.mode("overwrite").saveAsTable(gold_rental_per_postalcode_table_name)