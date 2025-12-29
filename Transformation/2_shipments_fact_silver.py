# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze To Silver : Data Cleaning And Transformation

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "shopvistaecommerce001", "Storage Account Name")
dbutils.widgets.text("container_name", "ecom-raw-data", "Container Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Bronze To Dataframe

# COMMAND ----------

df = spark.readStream \
.format("delta") \
.table(f"{catalog_name}.bronze.brz_order_shipments")
display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Transformations And Cleaning

# COMMAND ----------

df = df.dropDuplicates(["order_id", "order_dt", "shipment_id"])


# convert carrier column to uppercase and trim whitespace
df = df.withColumn('carrier', F.upper(F.col('carrier'))).withColumn('carrier', F.trim(F.col('carrier')))

#Add `processed_time` column with the current timestamp
df = df.withColumn(
    "processed_time", F.current_timestamp()
)



# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

silver_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/silver/fact_order_shipments/"
print(silver_checkpoint_path)

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchId):
    table_name = f"{catalog_name}.silver.slv_order_shipments"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("silver_table").merge(
            microBatchDF.alias("batch_table"),
            "silver_table.order_id = batch_table.order_id AND silver_table.order_dt = batch_table.order_dt AND silver_table.shipment_id = batch_table.shipment_id",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
       

# COMMAND ----------

df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_silver
).format("delta").option("checkpointLocation", silver_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()