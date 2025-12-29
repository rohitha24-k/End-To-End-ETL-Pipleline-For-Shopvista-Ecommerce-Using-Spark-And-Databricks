# Databricks notebook source
# MAGIC %md
# MAGIC # From Silver To Gold : Aggregation And KPI Tables

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
print(catalog_name, storage_account_name, container_name)

# COMMAND ----------

# readChangeFeed flag is used to read the change feed (_change_type column mainly)
df = spark.readStream \
.format("delta") \
.option("readChangeFeed", "true") \
.table(f"{catalog_name}.silver.slv_order_shipments")
df.limit(10).display()

# COMMAND ----------

df_union = df.filter("_change_type IN ('insert', 'update_postimage')")

# COMMAND ----------

#Add `carrier_group`: Domestic: ECOMEXPRESS, DELHIVERY, XPRESSBEES,BLUEDART.International: All other carriers.
df_union = df_union.withColumn(
    "carrier_group",
    F.when(
        F.col("carrier").isin("ECOMEXPRESS", "DELHIVERY", "XPRESSBEES", "BLUEDART"),
        F.lit("Domestic")
    ).otherwise(F.lit("International"))
)

# Add `is_weekend_shipment` flag:True if `order_dt` is Saturday or Sunday, else False.
df_union = df_union.withColumn(
    "is_weekend_shipment",
    F.when(F.weekday("order_dt").isin(5, 6), F.lit(True)).otherwise(F.lit(False))
)
df_union.limit(5).display()  


# COMMAND ----------

orders_gold_df = df_union.select(
    F.col("order_dt").alias("order_date"),
    F.col("order_id").alias("transaction_id"),
    F.col("shipment_id"),
    F.col("is_weekend_shipment"),
    F.col("carrier"),
    F.col("carrier_group"),
    )

# COMMAND ----------

gold_checkpoint_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoint/gold/fact_order_shipments/"
print(gold_checkpoint_path)

def upsert_to_gold(microBatchDF, batchId):
    table_name = f"{catalog_name}.gold.gld_fact_order_shipments"
    if not spark.catalog.tableExists(table_name):
        print("creating new table")
        microBatchDF.write.format("delta").mode("overwrite").saveAsTable(table_name)
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        deltaTable = DeltaTable.forName(spark, table_name)
        deltaTable.alias("gold_table").merge(
            microBatchDF.alias("batch_table"),
            "gold_table.transaction_id = batch_table.transaction_id AND gold_table.order_date = batch_table.order_date AND gold_table.shipment_id = batch_table.shipment_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

orders_gold_df.writeStream.trigger(availableNow=True).foreachBatch(
    upsert_to_gold
).format("delta").option("checkpointLocation", gold_checkpoint_path).option(
    "mergeSchema", "true"
).outputMode(
    "update"
).trigger(
    once=True
).start().awaitTermination()