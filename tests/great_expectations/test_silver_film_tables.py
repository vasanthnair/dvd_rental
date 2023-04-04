# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# Silver Film Table
silver_film_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_film')
ge_silver_film_df = SparkDFDataset(silver_film_df)
expect1 = ge_silver_film_df.expect_column_values_to_not_be_null("film_id")
assert expect1.success
expect2 = ge_silver_film_df.expect_column_values_to_be_unique("film_id")
assert expect2.success
expect3 = ge_silver_film_df.expect_column_values_to_be_of_type('film_description','StringType')
assert expect3.success
expect4 = ge_silver_film_df.expect_column_values_to_be_in_set(column="film_rating", value_set=['PG', 'R', 'G', 'NC-17', 'PG-13'])
assert expect4.success
expect5 = ge_silver_film_df.expect_column_values_to_be_in_set(column="film_suitable_for_children", value_set=[True, False])
assert expect5.success


# Silver Film Category Table
silver_film_category_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_film_category')
ge_silver_film_category_df = SparkDFDataset(silver_film_category_df)
expect6 = ge_silver_film_category_df.expect_column_values_to_not_be_null("film_id")
assert expect6.success
expect7 = ge_silver_film_category_df.expect_column_values_to_be_unique("film_id")
assert expect7.success
expect8 = ge_silver_film_category_df.expect_column_values_to_be_of_type('category_id','ShortType')
assert expect8.success
expect9 = ge_silver_film_category_df.expect_column_values_to_be_in_set(column="category_id", value_set=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
assert expect9.success
expect10 = ge_silver_film_category_df.expect_table_row_count_to_equal(1000)
assert expect10.success

# Silver Category Table
silver_category_df = spark.read.table('hive_metastore.dvd_rental_group_7.silver_category')
ge_silver_category_df = SparkDFDataset(silver_category_df)
expect11 = ge_silver_category_df.expect_column_values_to_not_be_null("category_id")
assert expect11.success
expect12 = ge_silver_category_df.expect_table_row_count_to_equal(16)
assert expect12.success
expect13 = ge_silver_category_df.expect_column_values_to_be_in_set(column="category_id", value_set=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
assert expect13.success
expect14 = ge_silver_category_df.expect_column_values_to_be_in_set(column="category_name", value_set=['Action', 'Animation', 'Children', 'Classics', 'Comedy', 'Documentary', 'Drama', 'Family', 'Foreign', 'Games', 'Horror', 'Music', 'New', 'Sci-Fi', 'Sports', 'Travel'])
assert expect14.success
expect15 = ge_silver_category_df.expect_column_values_to_be_of_type('category_name','StringType')
assert expect15.success
