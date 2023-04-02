# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_customer as select * from (
# MAGIC 
# MAGIC   with customer_count_table as (
# MAGIC     select 
# MAGIC       customer_id,
# MAGIC       count(*) as customer_transaction_count
# MAGIC     from hive_metastore.dvd_rental_group_7.silver_rental
# MAGIC     group by customer_id
# MAGIC   ),
# MAGIC   customer_clean as (
# MAGIC     select distinct
# MAGIC       customer_id,
# MAGIC       first_name as customer_first_name,
# MAGIC       last_name as customer_last_name,
# MAGIC       email as customer_email,
# MAGIC       address_id,
# MAGIC       activebool as customer_active,
# MAGIC       create_date as customer_create_date
# MAGIC     from hive_metastore.dvd_rental_group_7.bronze_customer
# MAGIC   )
# MAGIC   select
# MAGIC     customer_clean.*,
# MAGIC     customer_count_table.customer_transaction_count
# MAGIC   from customer_clean
# MAGIC   left join customer_count_table on customer_clean.customer_id = customer_count_table.customer_id
# MAGIC )
