# 🚀 ShopVista E-Commerce Data Engineering & BI Project

## Overview
An end-to-end **Data Engineering and BI pipeline** built using **Azure, Databricks, Delta Lake, and Power BI**.  
The project follows the **Medallion Architecture (Bronze → Silver → Gold)** to deliver scalable, reliable, and BI-ready datasets.

Microsoft PowerBI - https://app.powerbi.com/view?r=eyJrIjoiNWY1ZjM4ZmUtNDJmMS00MmYxLWIzMTUtZTM5ZTRmYmEzOWE1IiwidCI6IjgwOGNjODNlLWE1NDYtNDdlNy1hMDNmLTczYTFlYmJhMjRmMyIsImMiOjEwfQ%3D%3D&pageName=6626ee380684e014cafa 

## Architecture
- Azure Data Lake Gen2 – Raw data storage  
- Azure Databricks (PySpark) – Data processing  
- Delta Lake – ACID-compliant tables  
- Databricks Autoloader – Incremental ingestion  
- Power BI – Reporting & dashboards  

## Data Layers
**Bronze (Ingestion)**  
- Raw ingestion from ADLS to Delta  
- Dimension tables: brands, products, categories, customers  
- Fact tables: orders, shipments, returns  
- Fact tables ingested incrementally using Autoloader  

**Silver (Transformation)**  
- Data cleaning and standardization  
- Dimension tables prepared for analytics  
- Fact tables processed using upsert logic with checkpoints  

**Gold (BI Ready)**  
- Final star schema for BI  
- Dimensions: product, customer, date  
- Facts: orders (daily), shipments & returns (monthly)  
- Business-friendly columns added (region, date_id, etc.)

## Orchestration
- Daily refresh: dimensions + fact orders (Bronze → Silver → Gold)  
- Monthly refresh: shipments and returns  
- Dependencies handled using Databricks Jobs  

## Reporting
- Power BI dashboards built on Gold layer tables  
- KPIs for sales, shipments, and returns

## Author
**Rohitha Konakalla**
Linkedin - www.linkedin.com/in/rohitha-konakalla
Email - rohithakonakalla96@gmail.com
