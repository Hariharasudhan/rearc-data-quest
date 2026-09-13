# 📋 Rearc Quest Take-Home Submission & Validation Sheet

**Candidate:** Hariharasudhan  
**Track:** Data Engineering Quest (Databricks / Lakeflow Edition)  
**Public Repository:** [GitHub - Hariharasudhan/rearc-data-quest](https://github.com/Hariharasudhan/rearc-data-quest) 

---

## 📂 1. Artifact Verification Checklist
The repository includes DAB config (`databricks.yml`), ingestion script (`/src/step1_ingestion.py`), DLT pipeline (`/src/rearc_dlt_pipeline.py`), design documentation (`PROCESS.md`), setup guidelines (`README.md`), and validation screenshots (`/screenshots/`).

## 🏛️ 2. The 21-Table Medallion Topology Reference
The pipeline materializes 21 distinct tables across three layers:
*   **Bronze (7 Tables):** Raw ingestion staging for BLS data, dimensions, and population JSON.
*   **Silver (6 Tables):** Cleaned, typed, and structured intermediate conformity tables including metadata optimization views.
*   **Gold (8 Tables):** Final business-ready analytical data products using both PySpark and Spark SQL engines.

## ⚡ 3. Quick-Start Execution & Deployment Pipeline
Deploy and execute the project using Databricks Asset Bundles (DAB):
## https://dbc-a1d25508-ad4e.cloud.databricks.com 
```bash
databricks auth login --host https://databricks.com
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run rearc_orchestration_job
```

## 📊 4. Interactive SQL Validation
Run queries against tables like `gold_us_population_stats`, `gold_top_productivity_years`, and `gold_productivity_vs_population_master` to verify assignment results directly inside your Databricks workspace.
