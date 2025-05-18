#  Medallion Architecture: Databricks ETL Pipeline

This project implements a full data pipeline using the **Medallion Architecture** (Bronze → Silver → Gold) with **Azure Blob Storage** as the source and **Delta Lake** as the storage format. The pipeline was built and orchestrated in **Databricks**.

##  Project Structure

```
medallion_architecture/
├── dataset/ # Raw CSV files (source data)
├── doc/ # Documentation and diagrams
├── src/ # Notebooks for each pipeline layer
│ ├── bronze_layer # Ingests raw CSV into Delta Lake
│ ├── silver_layer # Cleans and transforms bronze data
│ └── gold_layer # Aggregates and prepares data for analytics
└── README.md # Project overview
```

---

##  Pipeline Layers

### 🥉 Bronze Layer
- Reads raw `.csv` files from Azure Blob Storage using a **SAS token**
- Standardizes column names
- Saves as **Delta Tables** to `/mnt/bronze/{table_name}`

### 🥈 Silver Layer
- Cleans data (e.g., removes `$`, converts dates to `yyyy-MM-dd`, handles nulls)
- Applies type conversions (e.g., string to double)
- Drops duplicates
- Saves cleaned Delta tables to `/mnt/silver/{table_name}`

### 🥇 Gold Layer
- Joins cleaned data from Silver
- Performs business-level aggregations:
  - 💰 Total sales per region and month
  - 🎯 Sales target achievements
  - 📦 Top-selling products and resellers
- Outputs analytics-ready Delta tables to `/mnt/gold/{table_name}`

---

## 📊 Technologies Used

- **Databricks Notebooks**
- **Azure Blob Storage (SAS-based access)**
- **Apache Spark**
- **Delta Lake**
- **PySpark (Spark SQL & DataFrame API)**
- **Databricks Jobs & Workflows**

---

## ⚙️ How to Run

1. Upload raw data into Azure Blob Storage under a container (e.g., `dataset`)
2. Mount blob container in Databricks (`/mnt/bronze`, `/mnt/silver`, `/mnt/gold`)
3. Generate a valid **SAS token** for reading data
4. Run each notebook in order:
   - `bronze_layer`
   - `silver_layer`
   - `gold_layer`
5. Orchestrate with a **Databricks Job** or **Workflow**

---

## 📌 Sample Outputs (Gold Layer)

- `total_sales_by_region_month`
- `sales_vs_target_by_employee_month`
- `top_resellers`
- `product_cost_vs_sales`

---

