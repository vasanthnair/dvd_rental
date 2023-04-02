# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.gold_fact_transaction as select * from (
# MAGIC   select distinct
# MAGIC     hive_metastore.dvd_rental_group_7.silver_rental.*,
# MAGIC     hive_metastore.dvd_rental_group_7.silver_inventory.film_id,
# MAGIC     hive_metastore.dvd_rental_group_7.silver_payment.payment_id,
# MAGIC     hive_metastore.dvd_rental_group_7.silver_payment.amount as payment_amount
# MAGIC   from hive_metastore.dvd_rental_group_7.silver_rental
# MAGIC   left join hive_metastore.dvd_rental_group_7.silver_inventory on hive_metastore.dvd_rental_group_7.silver_rental.inventory_id = hive_metastore.dvd_rental_group_7.silver_inventory.inventory_id
# MAGIC   left join hive_metastore.dvd_rental_group_7.silver_payment on hive_metastore.dvd_rental_group_7.silver_rental.rental_id = hive_metastore.dvd_rental_group_7.silver_payment.rental_id
# MAGIC   order by rental_id
# MAGIC )
