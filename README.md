# 🇮🇳 India Economic Data Platform

A production-grade data pipeline for ingesting and validating **Reserve Bank of India (RBI) Issue Department** monetary statistics using **Dagster, Snowflake, and dbt**.

This project demonstrates how to build **robust, incremental, and observable pipelines** for **Excel-based public data sources** where APIs are unavailable or gated.

---

## 📌 Key Features

- Incremental ingestion from RBI Excel snapshots
- Idempotent RAW data loading into Snowflake
- Domain-aware data quality checks using Dagster
- dbt staging models with contracts and tests
- Explicit Dagster ↔ dbt lineage
- Secure configuration via environment variables
- Single job execution with clear asset lineage

---

## Data Access Constraints

RBI DBIE datasets may require authentication or manual access at times.
This project demonstrates how to build resilient pipelines that:

- ingest historical snapshots

- detect future availability

- fail safely without breaking downstream systems

---

## Why Excel as source?

RBI publishes official monetary statistics primarily as Excel reports rather than versioned APIs.
This project demonstrates how to productionize Excel-based financial data using modern data engineering practices.

---

## Data Quality Guarantees

This pipeline enforces data quality using Dagster asset checks, ensuring:
- non-null reporting periods
- uniqueness of reporting dates
- comparison of balances
- Positive value 

---

## 🏗️ Architecture Overview

```text
RBI Excel (Manual Download)
        |
        v
Dagster Asset (Ingestion + Incremental Logic)
        |
        v
Snowflake RAW (rbi_issue_department_raw)
        |
        v
Dagster Asset Checks (Data Quality)
        |
        v
dbt Staging Model (stg_rbi_issue_department)
```

---

## How to run the project?
Just install the dependencies and run the below command. \
`dagster dev -w workspace.yml`