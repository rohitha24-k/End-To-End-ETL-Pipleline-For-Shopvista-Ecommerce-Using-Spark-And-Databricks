## 🥉 Bronze Layer – Ingestion

This folder represents the **Bronze layer** of the Medallion Architecture.

### 📌 Purpose
The Bronze layer is responsible for **raw data ingestion** from source systems into the data platform with minimal transformation.  
It preserves data in its original structure to enable traceability, reprocessing, and auditability.

---

### 📥 Data Sources
Raw data is ingested from **Azure Data Lake Storage (ADLS)** into **Azure Databricks**.

---

### 📊 Tables Ingested

#### Dimension Tables
- `dim_brands`
- `dim_products`
- `dim_categories`
- `dim_customers`
- `dim_date`

#### Fact Tables
- `fact_order_items`
- `fact_order_returns`
- `fact_order_shipments`

---

### ⚙️ Ingestion Process
1. Read raw files from **ADLS**
2. Load data into **Databricks**
3. Store data in **Delta format**
4. Apply only basic validations
5. No business logic or aggregations are applied at this stage

---

### 🧱 Output
- Raw datasets stored as **Delta tables**
- Serves as the foundation for the **Silver (Transformation)** layer

---

### 🛠 Technologies Used
- Azure Data Lake Storage (ADLS)
- Azure Databricks
- Delta Lake
- PySpark
