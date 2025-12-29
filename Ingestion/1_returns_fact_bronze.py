# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "shopvistaecommerce001", "Storage Account Name")
dbutils.widgets.text("container_name", "ecom-raw-data", "Container Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

print(catalog_name, storage_account_name, container_name)

# COMMAND ----------

# Azure Data Lake Storage - ADLS Gen2
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/order_returns/landing/"

# Checkpoint folders for streaming (bronze, silver, gold)
bronze_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_returns/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoLoader

# COMMAND ----------

spark.readStream \
 .format("cloudFiles") \
 .option("cloudFiles.format", "csv")  \
 .option("cloudFiles.schemaLocation", bronze_checkpoint_path) \
 .option("cloudFiles.schemaEvolutionMode", "rescue") \
 .option("header", "true") \
 .option("cloudFiles.inferColumnTypes", "true") \
 .option("rescuedDataColumn", "_rescued_data") \
 .option("cloudFiles.includeExistingFiles", "true")  \
 .option("pathGlobFilter", "*.csv") \
 .load(adls_path) \
 .withColumn("ingest_timestamp", F.current_timestamp()) \
 .withColumn("source_file", F.col("_metadata.file_path")) \
 .writeStream \
 .outputMode("append") \
 .option("checkpointLocation", bronze_checkpoint_path) \
 .trigger(availableNow=True) \
 .toTable(f"{catalog_name}.bronze.brz_order_returns") \
 .awaitTermination()

# COMMAND ----------

display(spark.sql(f"SELECT max(order_dt) FROM {catalog_name}.bronze.brz_order_returns"))
display(spark.sql(f"SELECT min(order_dt) FROM {catalog_name}.bronze.brz_order_returns"))

# COMMAND ----------



# COMMAND ----------

display(
    spark.sql(
        f"SELECT count(*) FROM CLOUD_FILES_STATE('abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/bronze/fact_order_returns/')"
    )
)