# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_film as select * from (
# MAGIC   select distinct
# MAGIC     film_id,
# MAGIC     title as film_title,
# MAGIC     description as film_description,
# MAGIC     release_year as film_release_year,
# MAGIC     language_id as film_language_id,
# MAGIC     length as film_length_minutes,
# MAGIC     round(length/60,1) as film_length_hours,
# MAGIC     case when film_length_hours>2 then true else false end as film_longer_than_2hrs,
# MAGIC     rating as film_rating,
# MAGIC     case when rating='G' then true else false end as film_suitable_for_children
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_film
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_film_category as select * from (
# MAGIC   select distinct
# MAGIC     film_id,
# MAGIC     category_id
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_film_category
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.silver_category as select * from (
# MAGIC   select distinct
# MAGIC     category_id,
# MAGIC     name as category_name
# MAGIC   from hive_metastore.dvd_rental_group_7.bronze_category
# MAGIC )
