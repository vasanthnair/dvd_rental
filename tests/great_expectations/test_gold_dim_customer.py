# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Gold Dim Customer Table
gold_dim_customer_df = spark.read.table('hive_metastore.dvd_rental_group_7.gold_dim_customer')
ge_gold_dim_customer_df = SparkDFDataset(gold_dim_customer_df)
expect1 = ge_gold_dim_customer_df.expect_column_values_to_not_be_null("customer_id")
assert expect1.success
expect2 = ge_gold_dim_customer_df.expect_column_values_to_be_unique("customer_id")
assert expect2.success
expect3 = ge_gold_dim_customer_df.expect_column_values_to_be_of_type('customer_email', 'StringType')
assert expect3.success
expect4 = ge_gold_dim_customer_df.expect_column_values_to_be_in_set(column="customer_active", value_set=[True, False])
assert expect4.success
expect5 = ge_gold_dim_customer_df.expect_column_values_to_be_of_type('customer_create_date', 'DateType')
assert expect5.success

