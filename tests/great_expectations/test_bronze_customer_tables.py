# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Bronze Customer Table
bronze_customer_df = spark.read.table('hive_metastore.dvd_rental_group_7.bronze_customer')
ge_bronze_customer_df = SparkDFDataset(bronze_customer_df)
expect1 = ge_bronze_customer_df.expect_table_columns_to_match_set(["customer_id","store_id", "first_name", "last_name", "email", "address_id", "activebool", "create_date", "last_update", "active"])
assert expect1.success
expect2 = ge_bronze_customer_df.expect_column_values_to_not_be_null("customer_id")
assert expect2.success
expect3 = ge_bronze_customer_df.expect_column_values_to_be_unique("customer_id")
assert expect3.success
expect4 = ge_bronze_customer_df.expect_column_values_to_be_in_set(column="activebool", value_set=[True, False])
assert expect4.success
expect5 = ge_bronze_customer_df.expect_table_row_count_to_equal(599)
assert expect5.success



