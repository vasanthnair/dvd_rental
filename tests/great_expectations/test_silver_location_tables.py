# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Silver Address Table
silver_address_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_address')
ge_silver_address_df = SparkDFDataset(silver_address_df)
expect1 = ge_silver_address_df.expect_column_values_to_not_be_null("address_id")
assert expect1.success
expect2 = ge_silver_address_df.expect_column_values_to_be_unique("address_id")
assert expect2.success
expect3 = ge_silver_address_df.expect_column_values_to_be_of_type('address_id', 'LongType')
assert expect3.success
expect4 = ge_silver_address_df.expect_column_values_to_be_of_type('address_phone', 'StringType')
assert expect4.success
expect5 = ge_silver_address_df.expect_table_row_count_to_equal(603)
assert expect5.success


# Silver City Table
silver_city_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_city')
ge_silver_city_df = SparkDFDataset(silver_city_df)
expect6 = ge_silver_city_df.expect_column_values_to_not_be_null("city_id")
assert expect6.success
expect7 = ge_silver_city_df.expect_column_values_to_be_unique("city_id")
assert expect7.success
expect8 = ge_silver_city_df.expect_table_row_count_to_equal(600)
assert expect8.success
expect9 = ge_silver_city_df.expect_column_values_to_be_of_type('city_name', 'StringType')
assert expect9.success
expect10 = ge_silver_city_df.expect_column_values_to_be_of_type('country_id', 'ShortType')
assert expect10.success

# Silver Country Table
silver_country_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_country')
ge_silver_country_df = SparkDFDataset(silver_country_df)
expect11 = ge_silver_country_df.expect_column_values_to_not_be_null("country_id")
assert expect11.success
expect12 = ge_silver_country_df.expect_column_values_to_be_unique("country_id")
assert expect12.success
expect13 = ge_silver_country_df.expect_table_row_count_to_equal(109)
assert expect13.success
expect14 = ge_silver_country_df.expect_column_values_to_be_of_type('country_name', 'StringType')
assert expect14.success
expect15 = ge_silver_country_df.expect_column_values_to_be_of_type('country_id', 'LongType')
assert expect15.success

# COMMAND ----------


