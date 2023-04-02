# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.gold_dim_location as select * from (
# MAGIC   select
# MAGIC     hive_metastore.dvd_rental_group_7.silver_address.*,
# MAGIC     hive_metastore.dvd_rental_group_7.silver_city.city_name,
# MAGIC     hive_metastore.dvd_rental_group_7.silver_country.country_name
# MAGIC   from hive_metastore.dvd_rental_group_7.silver_address
# MAGIC   left join hive_metastore.dvd_rental_group_7.silver_city on hive_metastore.dvd_rental_group_7.silver_address.city_id = hive_metastore.dvd_rental_group_7.silver_city.city_id
# MAGIC   left join hive_metastore.dvd_rental_group_7.silver_country on hive_metastore.dvd_rental_group_7.silver_city.country_id = hive_metastore.dvd_rental_group_7.silver_country.country_id
# MAGIC   order by address_id
# MAGIC )
