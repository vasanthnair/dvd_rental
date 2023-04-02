# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_rental as select * from (
# MAGIC   select distinct
# MAGIC     rental_id,
# MAGIC     inventory_id,
# MAGIC     customer_id,
# MAGIC     rental_date as rental_dt,
# MAGIC     return_date as return_dt,
# MAGIC     year(rental_date) as rental_year,
# MAGIC     month(rental_date) as rental_month,
# MAGIC     day(rental_date) as rental_day,
# MAGIC     year(return_date) as return_year,
# MAGIC     month(return_date) as return_month,
# MAGIC     day(return_date) as return_day,
# MAGIC     timestampdiff(day, rental_date, return_date) as rental_duration_days
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_rental 
# MAGIC   order by return_dt
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_payment as select * from (
# MAGIC   select distinct
# MAGIC     payment_id,
# MAGIC     customer_id,
# MAGIC     rental_id,
# MAGIC     amount
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_payment
# MAGIC   order by payment_id
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_inventory as select * from (
# MAGIC   select distinct
# MAGIC       inventory_id,
# MAGIC       film_id
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_inventory
# MAGIC   order by inventory_id
# MAGIC )
