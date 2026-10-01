# Z2D DWDF Mini Project 02 - ABC Hub: Evolving the Data Platform

Continuation of [`miniproject1`](../miniproject1) - same business scenario, data model,
and star schema, now orchestrated by Apache Airflow with a real Bronze/Silver/Gold
analytical database instead of NiFi-driven staging tables.

### Project Structure

```
miniproject2/
├── README.md                          # This file
├── report/mini_project_02_report.md   # Full report - Q1-Q4, with Mermaid diagrams
├── sql/
│   ├── bronze/
│   │   ├── 00_setup_fdw.sql           # postgres_fdw: exposes abc_hub_operational inside abc_hub_analytical
│   │   ├── 01_create_bronze_schema.sql
│   │   └── extract/*.sql              # Jinja-templated incremental extracts (5 files, grouped by domain)
│   ├── silver/
│   │   ├── 01_create_silver_schema.sql
│   │   └── 02-06_transform_*.sql      # Bronze -> Silver cleansing/denormalising (5 files)
│   ├── gold/
│   │   ├── 01_create_gold_schema.sql  # dim_*/fact_* (Mini Project 01's star schema, moved to gold)
│   │   ├── 02_load_dimensions.sql     # SCD1/SCD2 dimension procedures
│   │   └── 03_load_facts.sql          # Fact aggregation procedures
│   ├── quality/
│   │   ├── 01_data_quality_checks.sql # Quarantine INSERTs (Data Validation stage)
│   │   └── checks/*.sql               # 7 SQLCheckOperator-ready assertion queries
│   └── audit/
│       ├── 01_create_audit_log.sql    # gold.etl_audit_log + etl_audit_step_log
│       ├── log_audit_start.sql / log_audit_end.sql
│       └── 02_verification_queries.sql # Idempotency/backfill evidence queries
├── dags/
│   └── abc_hub_medallion_pipeline.py  # The one DAG
└── screenshots/                        # 9 real captured PNGs + README.md documenting each
                                        # (incl. 2 real bugs found and fixed while verifying)
```

### Software Versions

| Software | Version | Purpose |
|---|---|---|
| PostgreSQL | 18.4 (same instance as Mini Project 01) | Operational + Analytical databases |
| Apache Airflow | >=3.3,<3.4 | Orchestration (matches `Week 10/airflow3_data_engineering_examples/requirements.txt`) |
| `apache-airflow-providers-common-sql` | latest compatible | `SQLExecuteQueryOperator`, `SQLCheckOperator` |
| `apache-airflow-providers-postgres` | latest compatible | `PostgresHook` (only used by the failure callback) |
| `postgres_fdw` (PostgreSQL contrib extension) | bundled with PostgreSQL | Cross-database access from `abc_hub_analytical` into `abc_hub_operational` |

### Setup

**1. Install Airflow** (bare-metal, same pattern as `Week 07/README.md`):

```bash
pip install "apache-airflow>=3.3,<3.4" "apache-airflow-providers-common-sql" "apache-airflow-providers-postgres"
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow standalone
```

**2. Build the analytical schema** (run in order, against `abc_hub_analytical` - the same
database Mini Project 01 created; this project does not need a new database):

```bash
psql -d abc_hub_analytical -f sql/bronze/00_setup_fdw.sql        # edit the user mapping credentials first
psql -d abc_hub_analytical -f sql/bronze/01_create_bronze_schema.sql
psql -d abc_hub_analytical -f sql/silver/01_create_silver_schema.sql
psql -d abc_hub_analytical -f sql/gold/01_create_gold_schema.sql
psql -d abc_hub_analytical -f sql/gold/02_load_dimensions.sql
psql -d abc_hub_analytical -f sql/gold/03_load_facts.sql
psql -d abc_hub_analytical -f sql/audit/01_create_audit_log.sql
```

If you are starting from a database that still has Mini Project 01's `public.dim_*`/
`fact_*`/`staging.stg_*` tables, either drop them first or point the FDW/Bronze/Silver setup
at a fresh `abc_hub_analytical` - the scripts above create `bronze`/`silver`/`gold` schemas
and do not touch `public`/`staging`.

**3. Add the Airflow Connection** (only one is needed - `postgres_fdw` makes the
operational database reachable from inside `abc_hub_analytical`):

- Connection Id: `postgres_analytical`
- Connection Type: `Postgres`
- Host: `localhost` / Schema: `abc_hub_analytical` / Login: `postgres` / Port: `5432`
- Password: your own PostgreSQL password, typed into the Airflow UI (or `airflow connections add`).
  It is not stored anywhere in this repo.

**Running Airflow with Docker Compose instead** (this is how I ran it): copy `.env.example`
to `.env`, fill in your own values (the file is git-ignored, and `docker-compose.yaml` will not
start without them), then `docker compose up`. When Airflow runs in a container and
PostgreSQL runs on your machine, use `host.docker.internal` as the connection host instead
of `localhost`.

**4. Deploy the DAG**: copy (or symlink) `dags/abc_hub_medallion_pipeline.py` and the `sql/`
folder into `$AIRFLOW_HOME/dags/` so the relative `template_searchpath` in the DAG file
resolves - keep the same `dags/` + `sql/` sibling layout shown above.

### Running

**A normal scheduled/manual run:**

```bash
airflow dags unpause abc_hub_medallion_pipeline
airflow dags trigger abc_hub_medallion_pipeline
```

**Idempotency check** (Question 3 Part D): after a run completes, run
`sql/audit/02_verification_queries.sql` (queries 2 and 3 specifically check for
duplicate fact rows and duplicate current SCD2 versions - both must return zero rows);
then re-process the same logical date with the backfill command below and
`--reprocess-behavior completed`, and confirm queries 2 and 3 still return zero rows.

**Backfill** (Question 4 Part B):

```bash
airflow backfill create --dag-id abc_hub_medallion_pipeline \
  --from-date 2026-08-20 --to-date 2026-08-22 --reprocess-behavior completed
```

`airflow dags backfill` was removed in Airflow 3.x in favour of the dedicated
`airflow backfill create` command. `--reprocess-behavior completed` is also
what forces a re-run of an already-completed date, which is how this project
demonstrates idempotency in practice: run a date once, then explicitly
backfill-reprocess that same date and confirm row counts don't change.

**Where to look:**
- Rejected rows: `SELECT * FROM silver.dq_rejected ORDER BY rejected_at DESC;`
- Audit trail: `SELECT * FROM gold.etl_audit_log ORDER BY start_time DESC;`
- Per-step detail: `SELECT * FROM gold.etl_audit_step_log ORDER BY logged_at DESC;`

### Assumptions

1. `abc_hub_operational` and `abc_hub_analytical` run on the same PostgreSQL server (as in
   Mini Project 01) - this is what makes a single `postgres_fdw` connection sufficient.
2. The `postgres_fdw` user mapping in `sql/bronze/00_setup_fdw.sql` uses placeholder
   credentials (`postgres`/`postgres`) - replace with your actual role before running.
3. Bronze/Silver scope is the same 20 operational tables Mini Project 01's NiFi flow
   ultimately touched (13 staged sources + 7 small lookup/join tables previously joined
   live) - `artist`, `content_artist`, `payment_method`, `courier`, `delivery`, and
   `recommendation` are out of scope, as they were in Mini Project 01.
4. `dim_date` is populated once (2020-2030) by `sql/gold/01_create_gold_schema.sql`, not
   per DAG run.
5. Airflow's `catchup=True` + a fixed `start_date` of 2026-01-01 is what enables backfill;
   do not change `start_date` after the DAG has run, or historical `run_id`s become
   inconsistent with the audit log.

### Technology Decision (summary - full analysis in the report)

Airflow orchestrates the entire pipeline end to end, including extraction, via
`postgres_fdw` - NiFi is not part of this implementation's runtime path, because both the
source and target databases are PostgreSQL. See `report/mini_project_02_report.md`
Question 1 for the full weighted decision matrix and Question 4's Final Technology
Decision for how that recommendation would change if ABC Hub's sources became
heterogeneous.
