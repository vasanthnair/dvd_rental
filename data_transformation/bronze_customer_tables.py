# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_customer AS 
# MAGIC (SELECT 
# MAGIC   customer.customer_id,
# MAGIC   customer.store_id,
# MAGIC   customer.first_name,
# MAGIC   customer.last_name,
# MAGIC   customer.email,
# MAGIC   customer.address_id,
# MAGIC   CAST(customer.activebool AS boolean) AS activebool,
# MAGIC   CAST(customer.create_date AS date) AS create_date,
# MAGIC   CAST(customer.last_update AS timestamp) AS last_update,
# MAGIC   customer.active 
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_customer
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<customer_id:bigint,store_id:smallint,first_name:string,last_name:string,email:string,address_id:smallint,activebool:boolean,create_date:date,last_update:string,active:bigint>>')) exploded_data AS customer)
# MAGIC   
