# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Bronze Address Table
bronze_address_df = spark.read.table('hive_metastore.dvd_rental_group_7.bronze_address')
ge_bronze_address_df = SparkDFDataset(bronze_address_df)
expect1 = ge_bronze_address_df.expect_column_values_to_not_be_null("address_id")
assert expect1.success
expect2 = ge_bronze_address_df.expect_column_values_to_be_increasing("address_id")
assert expect2.success
expect3 = ge_bronze_address_df.expect_table_row_count_to_equal(603)
assert expect3.success
expect4 = ge_bronze_address_df.expect_column_values_to_be_of_type('district', 'StringType')
assert expect4.success
expect5 = ge_bronze_address_df.expect_column_values_to_be_of_type('address', 'StringType')
assert expect5.success

# Bronze City Table
bronze_city_df = spark.read.table('hive_metastore.dvd_rental_group_7.bronze_city')
ge_bronze_city_df = SparkDFDataset(bronze_city_df)
expect6 = ge_bronze_city_df.expect_column_values_to_not_be_null("city_id")
assert expect6.success
expect7 = ge_bronze_city_df.expect_column_values_to_be_increasing("city_id")
assert expect7.success
expect8 = ge_bronze_city_df.expect_table_row_count_to_equal(600)
assert expect8.success
expect9 = ge_bronze_city_df.expect_column_values_to_be_of_type('city', 'StringType')
assert expect9.success
expect10 = ge_bronze_city_df.expect_column_values_to_be_of_type('last_update', 'TimestampType')
assert expect10.success

# Bronze Country Table
bronze_country_df = spark.read.table('hive_metastore.dvd_rental_group_7.bronze_country')
ge_bronze_country_df = SparkDFDataset(bronze_country_df)
expect11 = ge_bronze_country_df.expect_column_values_to_not_be_null("country_id")
assert expect11.success
expect12 = ge_bronze_country_df.expect_column_values_to_be_increasing("country_id")
assert expect12.success
expect13 = ge_bronze_country_df.expect_table_row_count_to_equal(109)
assert expect13.success
expect14 = ge_bronze_country_df.expect_column_values_to_be_of_type('country_id', 'LongType')
assert expect14.success
expect15 = ge_bronze_country_df.expect_column_values_to_be_of_type('country', 'StringType')
assert expect15.success

# COMMAND ----------


