# IMAGO Data Coding Challenge Solution

## Overview
This repository contains my solution to the IMAGO Data Coding Challenge, focused on ensuring clean, accurate revenue data for the **IMAGO_Matrix_Metrics_2025** dashboard. It includes:

- **Data Analysis** of mock CSVs to surface quality issues  
- A **revised ETL pipeline** (SSIS + dbt) to enforce data quality, capture payments, and flag placeholders  
- A **migration plan** to modern tools (dbt, Airflow, Snowflake) with risk mitigation and a phased cutover  

A PDF report summarizing the approach is included in `docs/`.

---

## Tasks and Deliverables

### Task 1: Data Analysis  
**Objective:** Analyze `invoices.csv`, `positions.csv`, and `customers.csv` to answer:
1. How many positions link to invoices missing payment info?  
2. Revenue attributed to placeholder media ID `100000000`?  
3. How many invoices have no positions attached?  

**Files:**  
- `analysis/queries.sql` — SQL queries for each question  
- `analysis/mock_data/` — Mock CSVs (~100 rows each)  
- `analysis/analysis.ipynb` — Jupyter Notebook with query results and visualizations  

**Approach:**  
- Used SQL `JOIN`s and aggregations on mock data  
- Highlighted key DQ issues: missing payments, dummy placeholders, orphan invoices  

---

### Task 2: Pipeline Proposal  
**Objective:** Design a hardened ETL (SSIS + dbt) to guarantee clean, valid, traceable revenue from raw CSVs to Gold‐layer fact tables.  

**Files:**  
- `pipeline/dbt_project/`  
  - **models/staging/**  
    - `stg_invoices.sql`, `stg_positions.sql`, `stg_payments.sql`  
  - **models/intermediate/**  
    - `int_invoice_status.sql`, `int_position_flags.sql`, `int_cross_validation.sql`  
  - **models/marts/**  
    - `fct_revenue.sql`, `dim_customers.sql`  
  - `schema.yml` — not_null, unique, expression tests  
  - **tests/**  
    - `custom_dq_invalid_kdnr.sql`  
    - `custom_dq_negative_amount.sql`  
    - `custom_dq_cross_table_kdnr.sql`  
- `pipeline/ssis_scripts/check_file_integrity.cs` — SSIS script for file validation  
- `docs/pipeline_diagram.png` — Flowchart of Raw → Bronze → Silver → Gold  

**Key Components:**  
1. **Raw Landing**  
   - **Backoffice UI/API** for entry validation (ReId, KdNr) + audit log (`dbo.entry_log`)  
   - **SSIS** procedures:  
     - `sp_CheckFileIntegrity` (empty/header‐only files)  
     - `sp_ValidateCsvSchema` (schema, null/invalid keys, placeholder detection)  
   - **Dead-letter queue** + SLA tracking (`dbo.dead_letter


