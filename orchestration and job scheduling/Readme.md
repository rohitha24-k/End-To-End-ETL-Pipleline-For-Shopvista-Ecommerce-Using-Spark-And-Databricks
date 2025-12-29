## ⏱ Orchestration & Job Scheduling

This layer manages the **end-to-end orchestration and scheduling** of data pipelines across the **Bronze, Silver, and Gold** layers, ensuring timely and reliable data availability for analytics and reporting.

---

## 🔗 Overall Pipeline Flow

ADLS
->
Bronze Layer (Ingestion)
->
Silver Layer (Transformation)
->
Gold Layer (BI Ready Tables)
->
Power BI Dashboards


Pipelines are orchestrated to strictly follow **layer dependencies** to ensure data consistency.

---

## 📊 Dimension Tables – Daily Refresh

Dimension tables are refreshed **daily** to keep reference data up to date.

### 🔁 Flow
ADLS
->
Bronze: Raw Dimension Tables
->
Silver: Cleaned & Standardized Dimensions
->
Gold: BI-Optimized Dimensions


### ✅ Key Details
- Connected across **Bronze → Silver → Gold**
- Daily scheduled refresh
- Used by all downstream fact tables
- Ensures consistent slicing and filtering in BI

---

## 📈 Fact Tables – Orders (Daily Refresh)

Order data is business-critical and requires **daily freshness**.

### 🔁 Flow
ADLS
->
Bronze: Raw Order Items
->
Silver: Incremental UPSERT with Checkpoints
->
Gold: Daily Aggregated Orders


### ✅ Key Details
- End-to-end layer dependency
- Daily scheduled jobs
- Incremental processing using **UPSERT**
- Checkpoints ensure fault tolerance and idempotency

  Daily refresh dim
 ->
  Daily refresh fact order items 
---

## 📦 Fact Tables – Returns (Monthly Refresh)

Return data is less volatile and follows a **monthly refresh strategy**.

### 🔁 Flow
ADLS
->
Bronze: Raw Returns
->
Silver: Incremental Processing
->
Gold: Monthly Aggregated Returns


### ✅ Key Details
- Monthly scheduled jobs
- Layer dependency maintained
- Optimized for trend and quality analysis

---

## 🚚 Fact Tables – Shipments (Monthly Refresh)

Shipment data follows the same orchestration logic as returns.

### 🔁 Flow
ADLS
->
Bronze: Raw Shipments
->
Silver: Incremental Processing
->
Gold: Monthly Aggregated Shipments


### ✅ Key Details
- Monthly refresh
- Incremental processing with checkpoints
- Optimized for logistics performance reporting

---

## ⚙️ Scheduling & Dependency Management

- Pipelines are scheduled using **Azure Databricks Jobs**
- Downstream jobs trigger only after upstream success
- Daily jobs:
  - Dimension tables
  - Order fact tables
- Monthly jobs:
  - Returns fact tables
  - Shipments fact tables
- Checkpoints enable safe re-runs without duplication

---

## 🛠 Technologies Used
- Azure Databricks Jobs  
- Azure Data Lake Storage (ADLS)  
- Delta Lake  
- PySpark  
- UPSERT / Merge Operations  
- Checkpoints
