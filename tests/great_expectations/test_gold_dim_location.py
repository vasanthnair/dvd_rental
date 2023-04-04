# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Gold Dim Location Table
gold_dim_location_df = spark.read.table('hive_metastore.dvd_rental_group_7.gold_dim_location')
ge_gold_dim_location_df = SparkDFDataset(gold_dim_location_df)

expect1 = ge_gold_dim_location_df.expect_column_values_to_not_be_null("address_id")
assert expect1.success
expect2 = ge_gold_dim_location_df.expect_column_values_to_be_unique("address_id")
assert expect2.success
expect3 = ge_gold_dim_location_df.expect_column_values_to_be_of_type('country_name', 'StringType')
assert expect3.success
expect4 = ge_gold_dim_location_df.expect_column_values_to_be_of_type('city_id', 'ShortType')
assert expect4.success
expect5 = ge_gold_dim_location_df.expect_column_values_to_be_increasing("address_id")
assert expect5.success


