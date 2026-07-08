# Databricks notebook source
# DBTITLE 1,duplicates in dataset..
# MAGIC %sql
# MAGIC --Select count(1), longitude, latitude
# MAGIC --from sandbox_general.bronze.rentals
# MAGIC --group by longitude, latitude
# MAGIC --having count(1) > 1 
# MAGIC Select * from sandbox_general.bronze.rentals
# MAGIC where longitude = 5.671673 and latitude = 50.860841

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Distinct crawlStatus
# MAGIC  from sandbox_general.bronze.rentals

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), deposit 
# MAGIC from sandbox_general.bronze.rentals GROUP BY deposit
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), registrationCost 
# MAGIC from sandbox_general.bronze.rentals GROUP BY registrationCost

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * 
# MAGIC from sandbox_general.bronze.airbnb

# COMMAND ----------

# DBTITLE 1,Select from silver
# MAGIC %sql
# MAGIC Select * From sandbox_general.silver.airbnb

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From sandbox_general.silver.rentals