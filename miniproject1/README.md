# ABC Hub - Modern Data Platform

## Mini Project 01: Designing a Modern Data Platform for ABC Hub

### Project Overview
This project implements a modern data platform for ABC Hub, an entertainment company providing digital streaming and physical media rental services. The solution includes:
- Enterprise Data Warehouse design (Data Vault + Star Schema)
- ETL pipeline implementation using Apache NiFi
- Analytical database in PostgreSQL
- Star schema dimensional models for three business scenarios

### Project Structure

```
miniproject1/
├── README.md                          # This file
├── report/
│   ├── mini_project_report.md         # Full report source (Questions 1-8)
│   └── mini_project_report.pdf        # Submission copy
├── sql/
│   ├── operational/
│   │   ├── 01_create_operational_db.sql    # Operational database schema
│   │   └── 02_sample_data.sql              # Small hand-written dataset for quick local testing only —
│   │                                        # NOT used for the submitted verification numbers (see below)
│   ├── analytical/
│   │   ├── 01_create_analytical_db.sql            # Shared dimension tables (dim_date, dim_customer, ...)
│   │   ├── 02_fact_customer_daily_activity.sql     # Scenario A fact table DDL
│   │   ├── 03_fact_content_monthly_performance.sql # Scenario B fact table DDL
│   │   ├── 04_fact_inventory_utilisation.sql       # Scenario C fact table DDL
│   │   ├── 05_populate_dim_date.sql                # Date dimension population (2020-2030)
│   │   ├── 06_etl_transformations.sql              # Superseded reference queries (direct operational->fact);
│   │   │                                            # kept for reference, NOT part of the executed pipeline
│   │   ├── 07_create_staging_tables.sql            # Staging schema — landing zone for NiFi extracts
│   │   ├── 08_load_dimensions_from_staging.sql     # Dimension load stored procedures (SCD1 + SCD2)
│   │   └── 09_load_facts_from_staging.sql          # Fact load stored procedures (Scenarios A/B/C)
│   ├── nifi/
│   │   └── extraction_queries.sql     # Reference SQL for each NiFi extraction processor (Task 1)
│   └── views/
│       └── analytical_views.sql       # BI-ready analytical views (top content, revenue, warehouse health, ...)
├── nifi/
│   └── ABC_Hub_ETL_Pipeline.json      # Exported NiFi flow definition
├── scripts/
│   ├── nifi_build_flow.sh             # Builds the flow's process-group skeleton via the NiFi REST API
│   ├── nifi_build_task1_extract.sh    # PG_01_Extraction processors
│   ├── nifi_build_task2_transform.sh  # PG_02_Transformation processors
│   ├── nifi_build_task3_load.sh       # PG_03_Staging_Load + PG_04_Warehouse_Load processors
│   ├── nifi_build_task4_errors.sh     # PG_05_Error_Handling processors
│   └── test_staging_load.sh           # Ad-hoc staging load smoke test
├── diagrams/
│   └── Q1/Q2/Q4/Q6/Q7(A,B,C) diagrams  # .drawio source + exported .png/.pdf, one set per question
└── screenshots/
    └── NiFi workflow, execution, populated tables, and sample query result screenshots
```

### Software Versions Used

| Software | Version | Purpose |
|----------|---------|---------|
| PostgreSQL | 18.4 | Operational and Analytical databases |
| Apache NiFi | 2.10.0 | ETL pipeline orchestration |
| Java | 21 (Temurin/Eclipse Adoptium) | Required by Apache NiFi |
| DataGrip / psql | Latest | Database management and verification queries |

### Installation Steps

#### 1. PostgreSQL Setup

The **operational** database is loaded from the official **ABC Hub Data Package** (26 tables, 98,700 rows, provided separately in the bootcamp repository under `Z2D DWDF Mini Project 01/ABC Hub Data Package/packages/`) — not from `sql/operational/02_sample_data.sql`, which is only a small hand-written dataset kept for quick local smoke-testing.

```bash
# Start PostgreSQL (adjust paths to your install)
pg_ctl start -D /path/to/pgsql/data

# Create and load the operational database from the official data package
createdb abc_hub_operational
psql -d abc_hub_operational -f <path-to-data-package>/ddl/operational_schema.sql

# Load each of the 26 CSVs in dependency order (see the data package's own README
# for the exact order — parent tables such as country/city/customer before the
# tables that reference them, e.g. rental/payment/streaming_session last)
psql -d abc_hub_operational -c "\copy <table_name> FROM '<path>/<table_name>.csv' WITH (FORMAT csv, HEADER true)"
```

> Three columns in the official CSVs (`content.duration_minutes`, `streaming_session.watch_duration`, `payment.subscription_id`/`rental_id`) are exported as floats (e.g. `"4.0"`) rather than plain integers. If a plain `\copy` fails on one of these tables with an `invalid input syntax for type integer` error, load that table via a temporary staging table with the affected column typed `NUMERIC`, then `INSERT ... SELECT ... ::integer` into the real table. See the assumptions section below.

```bash
# Create the analytical database and schema
createdb abc_hub_analytical
psql -d abc_hub_analytical -f sql/analytical/01_create_analytical_db.sql
psql -d abc_hub_analytical -f sql/analytical/02_fact_customer_daily_activity.sql
psql -d abc_hub_analytical -f sql/analytical/03_fact_content_monthly_performance.sql
psql -d abc_hub_analytical -f sql/analytical/04_fact_inventory_utilisation.sql
psql -d abc_hub_analytical -f sql/analytical/05_populate_dim_date.sql

# Create the staging layer and the dimension/fact load procedures
psql -d abc_hub_analytical -f sql/analytical/07_create_staging_tables.sql
psql -d abc_hub_analytical -f sql/analytical/08_load_dimensions_from_staging.sql
psql -d abc_hub_analytical -f sql/analytical/09_load_facts_from_staging.sql

# Create the BI-ready analytical views
psql -d abc_hub_analytical -f sql/views/analytical_views.sql
```

`sql/analytical/06_etl_transformations.sql` is an earlier direct operational→fact approach kept for reference; it is **not** part of the executed pipeline — the staging-based flow (`07`→`08`→`09`, driven by NiFi) is what actually runs and produced the verified numbers below.

#### 2. Apache NiFi Setup

```bash
# Download Apache NiFi 2.10.0 from https://nifi.apache.org/download.html
# Extract to a local directory, e.g. C:\nifi

cd C:\nifi
bin\nifi.cmd start          # Windows
# or: bin/nifi.sh start     # Linux/macOS

# Check status
bin\nifi.cmd status

# Get the auto-generated login (first start only)
findstr /i "Generated Username" logs\nifi-app.log
findstr /i "Generated Password" logs\nifi-app.log
```

Access the NiFi UI at **http://127.0.0.1:8080/nifi** (this instance runs on plain HTTP; NiFi can also be configured for HTTPS on 8443 depending on `conf/nifi.properties`).

The PostgreSQL JDBC driver (`postgresql-42.7.x.jar`) needs to be available to NiFi — place it under `<NIFI_HOME>/jdbc/` and point the `DBCPConnectionPool` controller services at it (see below).

#### 3. Database Configuration

**Operational Database:**
- Host: localhost / Port: 5432 / Database: `abc_hub_operational` / Username: `postgres`

**Analytical Database:**
- Host: localhost / Port: 5432 / Database: `abc_hub_analytical` / Username: `postgres`

### Executing the NiFi Workflow

1. Import `nifi/ABC_Hub_ETL_Pipeline.json` as a new Process Group (canvas → **Process Group** → **Import**).
2. Configure the two `DBCPConnectionPool` controller services (exported flow definitions do not carry credentials):
   - `OperationalDB_Pool` → `jdbc:postgresql://localhost:5432/abc_hub_operational`
   - `AnalyticalDB_Pool` → `jdbc:postgresql://localhost:5432/abc_hub_analytical`
   - Driver location: the JDBC jar path from step 2 above; Driver class: `org.postgresql.Driver`
3. Enable both controller services.
4. Start every processor across all 5 process groups (select-all → **Start** in each group), then right-click `TRIGGER_Start_ETL_Batch` (inside `PG_01_Extraction`) → **Run Once** to kick off the batch immediately rather than waiting for its 2 AM cron schedule. The 6 incremental extractors poll independently every 30 seconds once started.
5. The pipeline runs in five stages, in this order:
   - `PG_01_Extraction` — connects to the operational DB, extracts all 13 source tables (7 full, 6 incremental)
   - `PG_02_Transformation` — validates mandatory fields, applies COALESCE defaults, deduplicates
   - `PG_03_Staging_Load` — UPSERTs cleansed records into the 13 staging tables
   - `PG_04_Warehouse_Load` — loads reference dimensions, then SCD2 dimensions (customer/content/inventory), then the three fact tables, strictly in that order
   - `PG_05_Error_Handling` — receives rejected/failed records from every other group, logs them, and writes them to both a flat file and `staging.etl_rejected_records`
6. Watch the connection queues drain to 0 and check the bulletin board (☰ menu → **Bulletin Board**) for any processor errors.
7. Check `staging.etl_rejected_records` in PostgreSQL and the flat-file directory at `C:/nifi/etl_logs/rejected` for any rejected records.

### ETL Execution Order

1. **Staging** (once per source table, UPSERT): all 13 source tables land in `staging.stg_*` first.
2. **Dimensions** (strictly before facts, so every fact row can resolve a surrogate key):
   - Reference dimensions (SCD1): `dim_device`, `dim_subscription_plan`, `dim_warehouse`, `dim_support_category`
   - SCD2 dimensions: `dim_customer` → `dim_content` → `dim_inventory_item`
   - `dim_date` is populated once, independently (2020-01-01 to 2030-12-31), not per run
3. **Facts** (after all dimensions have loaded):
   - `fact_customer_daily_activity`
   - `fact_content_monthly_performance`
   - `fact_inventory_utilisation`

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Data Vault for integration layer | Complete history, audit trail, regulatory compliance |
| Star Schema for presentation layer | Query performance, BI tool compatibility, self-service |
| SCD Type 2 on Customer, Content, Inventory | Track attribute changes over time (plan upgrades, condition changes) |
| Composite primary keys on facts | Enforces grain; prevents duplicate loading |
| UPSERT (staging) / SCD comparison (dimensions) / ON CONFLICT (facts) | Idempotency at every layer — safe re-execution without duplicates |
| Metadata-driven generic pipeline (PG_02-PG_04 shared across all 13 sources) | Avoids ~13x duplication of near-identical processors; per-source logic lives in one tagger |
| Daily batch processing, incremental extraction on high-volume tables | Balances freshness with system performance |

### Assumptions

1. All operational systems use PostgreSQL as their database
2. Data refresh frequency is daily (overnight batch at 02:00)
3. Historical data is retained indefinitely
4. The analytical database is on the same server (can be separated for production)
5. Time zone is UTC for all timestamps
6. A customer's "home" address is used for geographic analysis
7. Content can belong to multiple genres; the alphabetically first genre is used as primary in dimensions
8. Inventory utilisation is calculated from purchase_date to the last date actually covered by the extracted data, not `CURRENT_DATE` — snapshotting every item up to today would fabricate rows for calendar days with no loaded activity
9. **Float-formatted integer columns**: `content.duration_minutes`, `streaming_session.watch_duration`, and `payment.subscription_id`/`rental_id` are exported in the source CSVs as floats (e.g. `"4.0"`) — a pandas-style export artefact rather than one of the intentionally planted data-quality issues. These are cast explicitly during extraction rather than relying on an implicit narrowing conversion.
10. **Status/type casing normalisation**: `customer.status`, `rental.status`, `payment.status`, `customer_subscription.status`, and `customer_address.address_type` contain real-world casing and whitespace inconsistencies (`'Active'`, `'ACTIVE'`, `' Active '`). These are normalised with `LOWER(TRIM(...))`/`INITCAP(...)` before any comparison, since the fact-load procedures and SCD2 change-detection both compare on exact string values.

### Verification Queries

After running the ETL pipeline, verify with these queries:

```sql
-- Check dimension counts
SELECT 'dim_customer' AS table_name, COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_content', COUNT(*) FROM dim_content
UNION ALL SELECT 'dim_inventory_item', COUNT(*) FROM dim_inventory_item
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date;

-- Check fact table populations
SELECT 'fact_customer_daily_activity' AS table_name, COUNT(*) FROM fact_customer_daily_activity
UNION ALL SELECT 'fact_content_monthly_performance', COUNT(*) FROM fact_content_monthly_performance
UNION ALL SELECT 'fact_inventory_utilisation', COUNT(*) FROM fact_inventory_utilisation;

-- Check for rejected records
SELECT source_table, reject_reason, COUNT(*) FROM staging.etl_rejected_records GROUP BY 1, 2;

-- Sample analytical query
SELECT * FROM vw_top_content ORDER BY lifetime_streams DESC LIMIT 15;
```

Against the full 98,700-row ABC Hub dataset, this pipeline produces `fact_customer_daily_activity` = 67,897, `fact_content_monthly_performance` = 7,099, `fact_inventory_utilisation` = 665,926, and 4 genuinely rejected records (one each in `stg_rental`, `stg_streaming_session`, `stg_review`, `stg_payment`) — verified idempotent across two consecutive full runs.
