# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_film AS
# MAGIC (SELECT 
# MAGIC   film.film_id,
# MAGIC   film.title,
# MAGIC   film.description,
# MAGIC   film.release_year,
# MAGIC   film.language_id,
# MAGIC   film.rental_duration,
# MAGIC   CAST(film.rental_rate AS numeric(4,2)) AS rental_rate,
# MAGIC   film.length,
# MAGIC   CAST(film.replacement_cost AS numeric(5,2)) AS replacement_cost,
# MAGIC   film.rating,
# MAGIC   CAST(film.last_update AS timestamp) AS last_update,
# MAGIC   CAST(from_json(film.special_features, 'array<string>') AS array<string>) AS special_features
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_film
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<film_id:bigint,title:string,description:string,release_year:bigint,language_id:smallint,rental_duration:smallint,rental_rate:double,length:smallint,replacement_cost:double,rating:string,last_update:string,special_features:string>>')) exploded_data AS film);
# MAGIC   
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_film_category AS
# MAGIC (SELECT 
# MAGIC   film_category.film_id, 
# MAGIC   film_category.category_id, 
# MAGIC   CAST(film_category.last_update AS timestamp) AS last_update
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_film_category
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<film_id:smallint,category_id:smallint,last_update:string>>')) exploded_data AS film_category);
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dvd_rental_group_7.bronze_category AS
# MAGIC (SELECT 
# MAGIC   category.category_id, 
# MAGIC   category.name, 
# MAGIC   CAST(category.last_update AS timestamp) AS last_update
# MAGIC FROM 
# MAGIC   dvd_rental_group_7._airbyte_raw_category
# MAGIC LATERAL VIEW 
# MAGIC   explode(from_json(_airbyte_data, 'array<struct<category_id:bigint,name:string,last_update:string>>')) exploded_data AS category);
# MAGIC 
# MAGIC   
