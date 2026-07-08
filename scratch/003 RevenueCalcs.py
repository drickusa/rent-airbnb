# Databricks notebook source
# DBTITLE 1,Revenue per listing Airbnb
# MAGIC %sql
# MAGIC Select listingPk, sum(price) as Revenue, source AS Source, latitude, longitude, GetDate() as insertDateTime, Cast(null as timestamp) as updatedDateTime 
# MAGIC From sandbox_general.silver.airbnb
# MAGIC group by all

# COMMAND ----------

# DBTITLE 1,Revenue per listing rentals
# MAGIC %sql
# MAGIC Select listingPk, sum(rentAmount) as Revenue, source AS Source, GetDate() as insertDateTime, Cast(null as timestamp) as updatedDateTime 
# MAGIC From sandbox_general.silver.rentals as r
# MAGIC group by all 

# COMMAND ----------

# DBTITLE 1,Revenue per postal code Airbnb
# MAGIC %sql
# MAGIC Select postalCode, sum(price) as Revenue, source AS Source, GetDate() as insertDateTime, Cast(null as timestamp) as updatedDateTime 
# MAGIC From sandbox_general.silver.airbnb
# MAGIC group by postalCode, source

# COMMAND ----------

# DBTITLE 1,Revenue per postal code rentals
# MAGIC %sql
# MAGIC Select postalCode as PostalCode, sum(rentAmount) as Revenue, source AS Source, GetDate() as InsertedDateTime, Cast(null as timestamp) as UpdatedDateTime 
# MAGIC From sandbox_general.silver.rentals
# MAGIC group by all 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select listingPk, webpageTitle as RentalName, sum(rentAmount) as Revenue, source AS Source, latitude, longitude, GetDate() as insertDateTime, Cast(null as timestamp) as updatedDateTime 
# MAGIC From sandbox_general.silver.rentals as r
# MAGIC group by all 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select  *
# MAGIC From sandbox_general.silver.rentals as r limit 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From sandbox_general.gold.airbnb_per_listing;
# MAGIC --Select * From sandbox_general.gold.airbnb_per_postalcode;
# MAGIC --Select * From sandbox_general.gold.rental_per_listing;
# MAGIC --Select * From sandbox_general.gold.rental_per_postalcode;