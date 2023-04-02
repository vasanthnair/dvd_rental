# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_rental AS
# MAGIC (SELECT 
# MAGIC   rental.rental_id, 
# MAGIC   CAST(rental.rental_date AS timestamp) AS rental_date,
# MAGIC   rental.inventory_id, 
# MAGIC   rental.customer_id, 
# MAGIC   CAST(rental.return_date AS timestamp) AS return_date,
# MAGIC   rental.staff_id, 
# MAGIC   CAST(rental.last_update AS timestamp) AS last_update
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_rental
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<rental_id:bigint,rental_date:string,inventory_id:bigint,customer_id:smallint,return_date:string,staff_id:smallint,last_update:string>>')) exploded_data AS rental);
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_payment AS
# MAGIC (SELECT 
# MAGIC   payment.payment_id, 
# MAGIC   payment.customer_id, 
# MAGIC   payment.staff_id, 
# MAGIC   payment.rental_id, 
# MAGIC   CAST(payment.amount AS numeric(5,2)) AS amount,
# MAGIC   CAST(payment.payment_date AS timestamp) AS payment_date
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_payment
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<payment_id:bigint,customer_id:smallint,staff_id:smallint,rental_id:bigint,amount:double,payment_date:string>>')) exploded_data AS payment);
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_inventory AS
# MAGIC (SELECT 
# MAGIC   inventory.inventory_id, 
# MAGIC   inventory.film_id, 
# MAGIC   inventory.store_id, 
# MAGIC   CAST(inventory.last_update AS timestamp) AS last_update
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_inventory
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<inventory_id:bigint,film_id:smallint,store_id:smallint,last_update:string>>')) exploded_data AS inventory);
