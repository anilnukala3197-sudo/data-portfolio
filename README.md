# Anil Kumar Nukala — Data Portfolio

📧 anilnukala3197@gmail.com | 🔗 [LinkedIn](https://www.linkedin.com/in/anil-kumar-nukala/) | 💻 [GitHub](https://github.com/anilnukala3197-sudo) | 📍 Davie, FL

Data professional with 6+ years of experience in data engineering, BI, and analytics across supply chain, healthcare, and financial domains. This portfolio showcases production-grade SQL, Python, and Snowflake work.

---

## SQL Projects

### 1. [Inventory Anomaly Detection](sql/01_inventory_anomaly_detection/inventory_anomaly_detection.sql)
Detects sudden fluctuations in warehouse inventory using statistical z-scores and day-over-day change thresholds. Surfaces items needing immediate procurement attention ranked by financial impact.

**Techniques:** CTEs, window functions (`AVG`, `STDDEV`, `LAG` over rolling windows), CASE-based severity classification, multi-level ranking with `ROW_NUMBER` and `NTILE`.

---

### 2. [Supplier Performance Scorecard](sql/02_procurement_supplier_performance/supplier_performance_analysis.sql)
Multi-dimensional supplier scorecard across on-time delivery, fill rate, invoice accuracy, and defect rate. Computes a weighted composite score and quarters-over-quarter trend.

**Techniques:** Multi-CTE pipeline, period-over-period comparisons, weighted composite scoring, `RANK` within tier, `FULL OUTER JOIN`, `NULLIF` for safe division.

---

### 3. [Financial Reconciliation](sql/03_financial_reconciliation/financial_reconciliation.sql)
Reconciles AP invoices between a WMS and ERP General Ledger. Classifies discrepancies (missing, amount mismatch, line count variance, tax variance) and generates an audit-ready exception report.

**Techniques:** `FULL OUTER JOIN`, `COALESCE`, discrepancy classification, vendor-level rolling summary, parameterized date binding.

---

## Python Projects

### 4. [ETL Pipeline — WMS to Snowflake](python/01_etl_pipeline/etl_pipeline.py)
End-to-end incremental ETL pipeline that extracts inventory transactions from SQL Server, applies transformation rules (type normalization, signed quantities, MD5 row hashing), and bulk-loads into Snowflake. Supports full and incremental runs with watermark tracking.

**Libraries:** `pandas`, `pyodbc`, `snowflake-connector-python`, `python-dotenv`  
**Patterns:** Incremental loads, watermark management, connection pooling, logging to file + console.

---

### 5. [Automated Report Delivery](python/02_automated_reporting/automated_report_delivery.py)
Configurable system that executes scheduled SQL queries, formats results into styled multi-tab Excel files (alternating rows, auto-fit columns, freeze panes, auto-filter), and delivers via SMTP email or SFTP. New reports are added by appending to the catalogue — no code changes needed.

**Libraries:** `pandas`, `openpyxl`, `paramiko`, `snowflake-connector-python`  
**Patterns:** Data-driven report catalogue, dynamic date parameters, delivery audit log.

---

### 6. [Data Quality Framework](python/03_data_quality_framework/data_quality_framework.py)
Rule-based validation engine for DataFrames. Ships with built-in checks (null, uniqueness, range, regex, referential integrity, freshness) and a severity system (CRITICAL blocks the pipeline, HIGH triggers alerts). Outputs a structured report and CSV export.

**Libraries:** `pandas`  
**Patterns:** Factory-function rules, dataclass results, severity-gated pipeline blocking, extensible rule catalogue.

---

## Snowflake Patterns

### 7. [Snowflake Ecosystem Patterns](snowflake/snowflake_patterns.sql)
Production-ready Snowflake patterns: Time Travel for recovery, Streams + Tasks for CDC pipelines, Materialized Views for BI performance, table clustering, dynamic data masking for PII, QUALIFY for window-function filtering, JSON FLATTEN, and a JavaScript stored procedure for monthly close.

---

## Tech Stack

| Category | Tools |
|---|---|
| Languages | SQL, Python |
| Databases | Snowflake, SQL Server, Oracle |
| ETL / Pipelines | Azure Data Factory, Apache Airflow, custom Python |
| BI / Visualization | Power BI, Tableau, SSRS |
| Cloud | AWS, Azure |
| Libraries | pandas, openpyxl, pyodbc, snowflake-connector-python, paramiko |
| Certifications | Microsoft Power BI Data Analyst Associate, AWS Cloud Practitioner, Oracle Analytics Specialist |
