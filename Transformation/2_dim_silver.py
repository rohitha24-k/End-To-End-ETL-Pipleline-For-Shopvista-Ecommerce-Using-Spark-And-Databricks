# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze To Silver : Data Cleaning And Transformations For Dimension Tables 
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

catalog_name = 'ecommerce'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brands
# MAGIC

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Triming extra spaces in `Brand_name`

# COMMAND ----------

# triming the extra spaces from brand_name
df_silver = df_bronze.withColumn('brand_name', F.trim(F.col('brand_name')))
df_silver.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Replacing `Brand_code` with only alpha numeric characters 

# COMMAND ----------

# keeping only alpha numeric characters in brand_code and removing other characters 
df_silver = df_silver.withColumn('brand_code', F.regexp_replace(F.col('brand_code'), '[^a-zA-Z0-9]', ''))
df_silver.show(10)

# COMMAND ----------

df_silver.select("category_code").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Replacing Anomalies

# COMMAND ----------

# Anomalies dictionary
anomalies = {
    "GROCERY": "GRCY",
    "BOOKS": "BKS",
    "TOYS": "TOY"
}

# PySpark replace is easy
df_silver = df_silver.replace(to_replace=anomalies, subset=["category_code"])

# ✅ Show results
df_silver.select("category_code").distinct().show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_brands)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category
# MAGIC

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removing Duplicates in `Category_code` 

# COMMAND ----------

# filtering duplicates from the column category code 
df_duplicates = df_bronze.groupBy("category_code").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

# dropping duplicates from the column category code 
df_silver = df_bronze.dropDuplicates(["category_code"])
df_silver.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing `Category_code` to Upper Case 

# COMMAND ----------

# converting category code into upper case
df_silver = df_silver.withColumn('category_code', F.upper(F.col('category_code')))
df_silver.show(10)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products
# MAGIC
# MAGIC

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products")


# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

# display only the column weight grams show only 5 rows 
display(df_bronze.select("weight_grams").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Replacing g with '' in `weight_grams` and converting type to integer 

# COMMAND ----------

# replace 'g' with '' and type is intergertype 
df_silver = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

# drop file_name column
df_silver = df_silver.drop("file_name")
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Replacing , with . in `length_cm` and converting type to float 

# COMMAND ----------

# column length_cm is in string type should convert in to float type and replace , with . 
df_silver = df_silver.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver.select("length_cm").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing `Category_code` to Upper Case 

# COMMAND ----------

# replace category code with upper case
df_silver = df_silver.withColumn('category_code', F.upper(F.col('category_code')))
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing `Brand_Code` to Upper Case

# COMMAND ----------

# replace brand code with upper case
df_silver = df_silver.withColumn('brand_code', F.upper(F.col('brand_code')))
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spelling mistakes in `material` column
# MAGIC

# COMMAND ----------

df_bronze.select("material").show(10)

# COMMAND ----------

# Fix spelling mistakes
df_silver = df_silver.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)
df_silver.select("material").distinct().show()

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Negative values in `rating_count`
# MAGIC

# COMMAND ----------

# display rating count up to 10 rows
display(df_silver.select("rating_count").limit(10))

# COMMAND ----------

df_silver.filter(F.col('rating_count')<0).select("rating_count").show(3)

# COMMAND ----------

# convert negative rating count positive
df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(F.lit(0))  # if null, replace with 0
)

# COMMAND ----------

# Check final cleaned data

df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_dim_products)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Handle NULL values in `customer_id` column
# MAGIC

# COMMAND ----------

null_count = df_bronze.filter(F.col("customer_id").isNull()).count()
null_count

# COMMAND ----------

# Drop rows where 'customer_id' is null
df_silver = df_bronze.dropna(subset=["customer_id"])

# Get row count
row_count = df_silver.count()
print(f"Row count after droping null values: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Handle NULL values in `phone` column
# MAGIC

# COMMAND ----------

null_count = df_silver.filter(F.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 

# COMMAND ----------

### Fill null values with 'Not Available'
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# sanity check (If any nulls still exist)
df_silver.filter(F.col("phone").isNull()).show()

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calendar/Date
# MAGIC

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove Duplicates
# MAGIC

# COMMAND ----------

# Find duplicate rows in the DataFrame
duplicates = df_bronze.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)

# COMMAND ----------

# Remove duplicate rows
df_silver = df_bronze.dropDuplicates(['date'])

# Get row count
row_count = df_silver.count()

print("Rows After removing Duplicates: ", row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `day_name` normalize casing
# MAGIC

# COMMAND ----------

# Capitalize first letter of each word in day_name
df_silver = df_silver.withColumn("day_name", F.initcap(F.col("day_name")))

df_silver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enhance quarter and week_of_year column

# COMMAND ----------

df_silver = df_silver.withColumn("week_of_year", F.abs(F.col("week_of_year")))  # Convert negative to positive

df_silver.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Enhance quarter and week_of_year column

# COMMAND ----------

df_silver = df_silver.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver = df_silver.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

df_silver.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename Column

# COMMAND ----------

# Rename a column
df_silver = df_silver.withColumnRenamed("week_of_year", "week")

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")