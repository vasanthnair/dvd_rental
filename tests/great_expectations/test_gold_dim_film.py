# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Gold Dim Film Table
gold_dim_film_df = spark.read.table('hive_metastore.dvd_rental_group_7.gold_dim_film')
ge_gold_dim_film_df = SparkDFDataset(gold_dim_film_df)

expect1 = ge_gold_dim_film_df.expect_column_values_to_not_be_null("film_id")
assert expect1.success
expect2 = ge_gold_dim_film_df.expect_column_values_to_be_unique("film_id")
assert expect2.success
expect3 = ge_gold_dim_film_df.expect_column_values_to_be_of_type('film_description', 'StringType')
assert expect3.success
expect4 = ge_gold_dim_film_df.expect_column_values_to_be_in_set(column="film_suitable_for_children", value_set=[True, False])
assert expect4.success
expect5 = ge_gold_dim_film_df.expect_column_values_to_be_increasing("film_id")
assert expect5.success


