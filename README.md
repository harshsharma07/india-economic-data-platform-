# india-economic-data-platform

RBI does not provide an official versioned API. This project demonstrates how to build resilient pipelines on top of semi-structured public data sources.

### Data Access Constraints

RBI DBIE datasets may require authentication or manual access at times.
This project demonstrates how to build resilient pipelines that:

ingest historical snapshots

detect future availability

fail safely without breaking downstream systems

### Why Excel as source?

RBI publishes official monetary statistics primarily as Excel reports rather than versioned APIs.
This project demonstrates how to productionize Excel-based financial data using modern data engineering practices.

### Data Quality Guarantees

This pipeline enforces data quality using Dagster asset checks, ensuring:
- non-null reporting periods
- uniqueness of reporting dates
- comparison of balances
- Positive value 

### How to run the project?
Just install the dependencies and run the below command.
`dagster dev -w workspace.yml`