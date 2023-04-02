# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_address as select * from (
# MAGIC   select distinct
# MAGIC     address_id,
# MAGIC     address,
# MAGIC     address2,
# MAGIC     district as address_district,
# MAGIC     city_id,
# MAGIC     postal_code as address_postal_code,
# MAGIC     phone as address_phone
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_address
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_city as select * from (
# MAGIC   select distinct
# MAGIC     city_id,
# MAGIC     city as city_name,
# MAGIC     country_id
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_city
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_country as select * from (
# MAGIC   select distinct
# MAGIC     country_id,
# MAGIC     country as country_name
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_country
# MAGIC )
