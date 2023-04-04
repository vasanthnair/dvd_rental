# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Gold Fact Transaction Table
gold_fact_transaction_df = spark.read.table('hive_metastore.dvd_rental_group_7.gold_fact_transaction')
ge_gold_fact_transaction_df = SparkDFDataset(gold_fact_transaction_df)

expect1 = ge_gold_fact_transaction_df.expect_column_values_to_not_be_null("rental_id")
assert expect1.success
expect2 = ge_gold_fact_transaction_df.expect_column_values_to_be_in_set(column="rental_year", value_set=[2005, 2006])
assert expect2.success
expect3 = ge_gold_fact_transaction_df.expect_column_values_to_be_of_type('rental_dt', 'TimestampType')
assert expect3.success
expect4 = ge_gold_fact_transaction_df.expect_table_row_count_to_equal(16048)
assert expect4.success
expect5 = ge_gold_fact_transaction_df.expect_column_values_to_be_increasing("rental_id")
assert expect5.success



