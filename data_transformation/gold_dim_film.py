# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace table hive_metastore.dvd_rental_group_7.gold_dim_film as select * from (
# MAGIC   select distinct
# MAGIC     hive_metastore.dvd_rental_group_7.silver_film.*,
# MAGIC     hive_metastore.dvd_rental_group_7.silver_category.category_name as film_category
# MAGIC   from hive_metastore.dvd_rental_group_7.silver_film
# MAGIC   left join hive_metastore.dvd_rental_group_7.silver_film_category on hive_metastore.dvd_rental_group_7.silver_film.film_id = hive_metastore.dvd_rental_group_7.silver_film_category.film_id
# MAGIC   left join hive_metastore.dvd_rental_group_7.silver_category on hive_metastore.dvd_rental_group_7.silver_film_category.category_id = hive_metastore.dvd_rental_group_7.silver_category.category_id
# MAGIC   order by film_id
# MAGIC )
