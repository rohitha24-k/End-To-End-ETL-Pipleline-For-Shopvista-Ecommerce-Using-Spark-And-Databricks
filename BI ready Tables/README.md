## 🥇 Gold Layer – BI Ready Tables

This folder represents the **Gold layer** of the Medallion Architecture.

### 📌 Purpose
The Gold layer contains **curated, analytics-ready tables** designed specifically for **business intelligence and reporting** use cases.  
At this stage, data is modeled, enriched, and optimized to support **Power BI dashboards and business KPIs**.

---

### 📊 Dimension Tables (BI Optimized)

To enable efficient slicing, filtering, and aggregation in BI tools, the following **final dimension tables** are created:

#### `dim_product`
- Combines **brand, product, and category** information
- Designed as a single conformed dimension for simplified reporting


  ### 📈 Fact Tables (Aggregated & BI Ready)
Final fact tables are **aggregated and optimized** for reporting needs.

#### `fact_orders_items_daily`
- Daily-level order metrics

  
**Key Characteristics:**
- Grain: **One row per day**
- Linked to `dim_product`, `dim_customer`, and `dim_date`
- Supports daily sales, revenue, and order volume analysis

---

#### `fact_shipments_monthly`
- Monthly-level shipment metrics

**Key Characteristics:**
- Optimized for logistics and delivery performance analysis

---

#### `fact_returns_monthly`
- Monthly-level return metrics

**Key Characteristics:**
- Enables return rate and quality analysis

---

### ⚙️ Gold Layer Processing Logic
1. Read cleaned data from **Silver layer**
2. Add **BI-specific columns** such as `region` and `date_id`
3. Create **conformed dimensions** (`product`, `customer`, `date`)
4. Aggregate fact tables at **daily and monthly grains**
5. Apply **UPSERT (merge) operations** to handle incremental updates
6. Use **checkpoints** to ensure reliable and idempotent processing
7. Store final outputs as **Delta tables** for BI consumption

---

### 🧱 Output
- BI-optimized **dimension tables**
- Aggregated and incremental **fact tables**
- Directly consumed by **Power BI dashboards**

---

### 🛠 Technologies Used
- Azure Databricks  
- Delta Lake  
- PySpark  
- UPSERT / Merge Operations  
- Checkpoints for incremental processing  
- Power BI
