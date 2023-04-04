# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Silver Rental Table
silver_rental_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_rental')
ge_silver_rental_df = SparkDFDataset(silver_rental_df)
expect1 = ge_silver_rental_df.expect_column_values_to_not_be_null("rental_id")
assert expect1.success
expect2 = ge_silver_rental_df.expect_column_values_to_be_unique("rental_id")
assert expect2.success
expect3 = ge_silver_rental_df.expect_column_values_to_be_in_set(column="rental_year", value_set=[2005, 2006])
assert expect3.success
expect4 = ge_silver_rental_df.expect_column_values_to_be_of_type('rental_id', 'LongType')
assert expect4.success
expect5 = ge_silver_rental_df.expect_column_values_to_be_of_type('rental_dt', 'TimestampType')
assert expect5.success



# Silver Payment Table
silver_payment_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_payment')
ge_silver_payment_df = SparkDFDataset(silver_payment_df)
expect6 = ge_silver_payment_df.expect_column_values_to_not_be_null("payment_id")
assert expect6.success
expect7 = ge_silver_payment_df.expect_column_values_to_be_unique("payment_id")
assert expect7.success
expect8 = ge_silver_payment_df.expect_table_row_count_to_equal(14596)
assert expect8.success
expect9 = ge_silver_payment_df.expect_column_values_to_be_of_type('amount','DecimalType')
assert expect9.success
expect10 = ge_silver_payment_df.expect_column_values_to_be_of_type('payment_id','LongType')
assert expect10.success

# Silver Inventory Table
silver_inventory_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_inventory')
ge_silver_inventory_df = SparkDFDataset(silver_inventory_df)
expect11 = ge_silver_inventory_df.expect_column_values_to_not_be_null("inventory_id")
assert expect11.success
expect12 = ge_silver_inventory_df.expect_table_row_count_to_equal(4581)
assert expect12.success
expect13 = ge_silver_inventory_df.expect_column_values_to_be_of_type('inventory_id','LongType')
assert expect13.success
expect14 = ge_silver_inventory_df.expect_column_values_to_be_of_type('film_id','ShortType')
assert expect14.success
expect15 = ge_silver_inventory_df.expect_column_values_to_be_unique("inventory_id")
assert expect15.success

# COMMAND ----------


