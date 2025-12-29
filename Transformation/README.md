## 🥈 Silver Layer – Transformation

This folder represents the **Silver layer** of the Medallion Architecture.

### 📌 Purpose
The Silver layer is responsible for **data cleaning, enrichment, and transformation**.  
- Dimension tables are **cleaned, standardized, and deduplicated** to ensure consistency.  
- Fact tables are **incrementally updated** using UPSERT operations with checkpoints, preserving historical data while enabling incremental processing.

---

### 📊 Tables Processed

#### Dimension Tables (Cleaned & Curated)
- `dim_brands`
- `dim_products`
- `dim_categories`
- `dim_customers`

**Key operations:**
- Remove duplicates  
- Standardize text fields (e.g., product names, categories)  
- Handle missing/null values  
- Apply consistent data types and formats  

#### Fact Tables (Incremental & UPSERT)
- `fact_order_items`
- `fact_order_returns`
- `fact_order_shipments`

**Key operations:**
- Apply **UPSERT (merge) operations** to handle incremental data  
- Maintain **checkpoints** for reliable streaming or batch updates  
- Preserve historical data while adding new or updated records  
- Minimal transformations; business logic applied carefully to maintain accuracy

---

### ⚙️ Transformation Process
1. Read raw Delta tables from **Bronze layer**
2. Apply cleaning and deduplication for **dimension tables**
3. Apply **UPSERT** and checkpoint logic for **fact tables**
4. Write transformed data into **Silver Delta tables**, ready for analytics and Gold layer consumption

---

### 🧱 Output
- Cleaned and standardized dimension tables
- Incrementally updated fact tables with checkpoints  
- Serves as the foundation for Gold layer (BI-ready tables)

---

### 🛠 Technologies Used
- Azure Databricks  
- Delta Lake  
- PySpark  
- UPSERT / Merge Operations  
- Checkpoints for incremental processing
