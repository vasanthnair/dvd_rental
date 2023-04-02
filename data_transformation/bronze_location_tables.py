# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_address AS
# MAGIC (SELECT 
# MAGIC   address.address_id, 
# MAGIC   address.address,
# MAGIC   address.address2,
# MAGIC   address.district,
# MAGIC   address.city_id,
# MAGIC   address.postal_code,
# MAGIC   address.phone,
# MAGIC   CAST(address.last_update AS timestamp) AS last_update
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_address
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<address_id:bigint,address:string,address2:string,district:string,city_id:smallint,postal_code:string,phone:string,last_update:string>>')) exploded_data AS address);
# MAGIC   
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_city AS
# MAGIC (SELECT 
# MAGIC   city.city_id, 
# MAGIC   city.city, 
# MAGIC   city.country_id, 
# MAGIC   CAST(city.last_update AS timestamp) AS last_update
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_city
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<city_id:bigint,city:string,country_id:smallint,last_update:string>>')) exploded_data AS city);
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_country AS
# MAGIC (SELECT 
# MAGIC   country.country_id, 
# MAGIC   country.country, 
# MAGIC   CAST(country.last_update AS timestamp) AS last_update
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_country
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<country_id:bigint,country:string,last_update:string>>')) exploded_data AS country);
# MAGIC   
