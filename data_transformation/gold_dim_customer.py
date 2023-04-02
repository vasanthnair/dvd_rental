# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.gold_dim_customer as select * from (
# MAGIC   select
# MAGIC     hive_metastore.dvd_rental_group_7.silver_customer.*,
# MAGIC     case when customer_transaction_count>1 then true else false end as repeat_customer,
# MAGIC     case when customer_transaction_count>30 then true else false end as high_frequenter_customer
# MAGIC   from hive_metastore.dvd_rental_group_7.silver_customer
# MAGIC )
