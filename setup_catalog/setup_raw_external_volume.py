# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC USE CATALOG ecommerce;                     
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;           
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS raw.raw_landing
# MAGIC   LOCATION 'abfss://ecom-raw-data@shopvistaecommerce001.dfs.core.windows.net/'
# MAGIC   COMMENT 'Raw ADLS Gen2 landing for ecommerce';