# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Bronze Film Table
bronze_film_df = spark.read.table('hive_metastore.dvd_rental_group_7.bronze_film')
ge_bronze_film_df = SparkDFDataset(bronze_film_df)
expect1 = ge_bronze_film_df.expect_table_columns_to_match_set(["film_id","title", "description", "release_year", "language_id", "rental_duration", "rental_rate", "length", "replacement_cost", "rating", "last_update", "special_features"])
assert expect1.success
expect2 = ge_bronze_film_df.expect_column_values_to_not_be_null("film_id")
assert expect2.success
expect3 = ge_bronze_film_df.expect_column_values_to_be_unique("film_id")
assert expect3.success
expect4 = ge_bronze_film_df.expect_column_values_to_be_in_set(column="rating", value_set=['PG', 'R', 'G', 'NC-17', 'PG-13'])
assert expect4.success
expect5 = ge_bronze_film_df.expect_table_row_count_to_equal(1000)
assert expect5.success


# Bronze Film Category Table
bronze_film_category_df = spark.read.table('hive_metastore.dvd_rental_group_7.bronze_film_category')
ge_bronze_film_category_df = SparkDFDataset(bronze_film_category_df)
expect6 = ge_bronze_film_category_df.expect_table_columns_to_match_set(["film_id", "category_id", "last_update"])
assert expect6.success
expect7 = ge_bronze_film_category_df.expect_column_values_to_not_be_null("film_id")
assert expect7.success
expect8 = ge_bronze_film_category_df.expect_column_values_to_be_unique("film_id")
assert expect8.success
expect9 = ge_bronze_film_category_df.expect_column_values_to_be_increasing("film_id")
assert expect9.success
expect10 = ge_bronze_film_category_df.expect_table_row_count_to_equal(1000)
assert expect10.success


# Bronze Category Table
bronze_category_df = spark.read.table('hive_metastore.dvd_rental_group_7.bronze_category')
ge_bronze_category_df = SparkDFDataset(bronze_category_df)
expect11 = ge_bronze_category_df.expect_table_columns_to_match_set(["category_id", "name", "last_update"])
assert expect11.success
expect12 = ge_bronze_category_df.expect_column_values_to_not_be_null("category_id")
assert expect12.success
expect13 = ge_bronze_category_df.expect_column_values_to_be_unique("category_id")
assert expect13.success
expect14 = ge_bronze_category_df.expect_column_values_to_be_increasing("category_id")
assert expect14.success
expect15 = ge_bronze_category_df.expect_table_row_count_to_equal(16)
assert expect15.success
expect16 = ge_bronze_category_df.expect_column_values_to_be_in_set(column="name", value_set=['Action', 'Animation', 'Children', 'Classics', 'Comedy', 'Documentary', 'Drama', 'Family', 'Foreign', 'Games', 'Horror', 'Music', 'New', 'Sci-Fi', 'Sports', 'Travel'])
assert expect16.success
