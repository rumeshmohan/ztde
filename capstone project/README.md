# PulseTrack — Data Warehousing Capstone

## Z2D DWDF Capstone: Enterprise Data Warehouse for an Ad-Tech Platform

### Project Overview

PulseTrack is a fictional performance-marketing / affiliate-tracking platform
(advertisers, campaigns, offers, creatives, partners, impressions, clicks,
conversions, multi-touch attribution, revenue adjustments). This project
designs and implements one continuous, end-to-end analytical platform over it:

`Source → Data Platform → Enterprise Data Warehouse → Data Mart → OLAP/Semantic Layer → Business Analytics`

The assessment is 10 sequentially-dependent questions (each deliverable is the
input to the next — see the brief, provided separately by Zuu Crew Machine
Learning Academy); this repo is built up question-by-question rather than as
10 unrelated mini-solutions.

**Where the source data lives:** the official PulseTrack dataset (11 raw
relational CSVs + PostgreSQL DDL, 2 MongoDB JSONL collections, and a
Kafka-style conversion event stream — ~958,000 records, one seeded ground
truth across all three sources) is **not duplicated into this project**. It is
provided separately in the bootcamp course repository:

```
../dw-fundamentals-bootcamp/Z2D DWDF Capstone Project/DE01Capstone/
├── data/           11 raw CSVs (impressions, clicks, conversions, ...)
├── ddl/            pulsetrack_source_schema.sql — RAW source schema (Bronze)
├── mongo/          advertiser_accounts.jsonl, partner_profiles.jsonl
└── stream/         pulsetrack_conversions.log + README_topic.md
```

(adjust the relative path if this project is ever moved elsewhere on disk —
what matters is that it points at that folder). The brief and reference
documents (data guide, ER diagram) are not included in this submission —
they're material provided by the course, not a deliverable.

### Project Structure

```
capstone project/
├── README.md                                  # This file
├── report/
│   └── pulsetrack_capstone_report.md          # THE report — one continuous solution, Q1-Q10
│                                               # (architecture/ER diagrams are inline Mermaid in
│                                               #  Q3 §1; every query result cited is pasted in as
│                                               #  a real table — no separate diagrams/ or
│                                               #  screenshots/ folder ended up being needed)
├── sql/
│   ├── audit/01_create_audit_log.sql          # Q6: pipeline_run_log, quarantine, watermark tables
│   ├── bronze/01_create_bronze_schema.sql     # Q4: 11 relational + 2 Mongo JSONB + stream tables
│   ├── silver/00-04_*.sql                     # Q4: parsing functions + cleansing/dedup loads
│   ├── gold/02-11_*.sql                       # Q2 DDL + Q4/Q5 dimension/fact/bridge loads (SCD2)
│   ├── mart/01-03_*.sql                       # Q7: Data Mart schema, load, calculated-metric views
│   ├── olap/01-02_*.sql                       # Q8: 3 ROLLUP cubes + every OLAP operation demonstrated
│   ├── quality/01-02_*.sql                    # Q6: data quality checks + reconciliation report
│   └── analytics/01_business_questions.sql    # Q9: all 10 business questions, Mart/OLAP only
├── python/
│   ├── config.py, db.py, parsing.py, audit.py # shared helpers
│   ├── extract_relational.py                  # Q4: full-load Bronze bootstrap (CSV/PostgreSQL source)
│   ├── extract_relational_incremental.py      # Q5: watermark-based incremental extraction
│   ├── extract_mongo.py, extract_stream.py    # Q4: JSON/Kafka-style ingestion
│   ├── run_pipeline.py                        # Q6: audit-instrumented full-pipeline driver
│   └── simulate_partial_pipeline.py           # Q10: mid-pipeline-failure/restart test (real, reusable)
└── dags/pulsetrack_medallion_pipeline.py      # Q3/Q4: Airflow 3.x TaskFlow DAG (syntax-checked only,
                                                #  not run against a live Airflow instance)
```

### Progress

See the progress table at the top of
[`report/pulsetrack_capstone_report.md`](report/pulsetrack_capstone_report.md).
**All ten questions are done.** KPI/requirements spec, source forensics + the
final Gold dimensional model, the technology decision record, a working
Bronze→Silver→Gold pipeline (Q4), incremental/idempotent processing,
late-arriving data, historical corrections and recovery (Q5), full data
quality checks, a two-stage reconciliation report and complete audit
coverage (Q6), a business-oriented Data Mart built from Gold with every
measure reconciled exactly (Q7), an OLAP cube / semantic model built
from the Mart — three hierarchy-scoped cubes using `ROLLUP`, every OLAP
operation (roll-up, drill-down, slice, dice, pivot, Top-N, %-contribution,
MoM/QoQ/YoY) demonstrated with real results (Q8), all ten Q1 §5
business questions answered from the Mart/OLAP layer alone, each with a
full Business KPI → OLAP → Mart → Gold → Silver → Bronze → Source
traceability chain (Q9), and a production-architecture defence against
eight real operational scenarios — four of them actually reproduced live
(duplicate file delivery, a simulated mid-pipeline crash + restart, a fresh
reconciliation-report run, a five-year-shaped query timed against the OLAP
cube) rather than only argued (Q10) — all implemented and run end-to-end
against the real dataset (not a sample) in a local PostgreSQL 16 Docker
instance. Every row is accounted for (loaded or explicitly quarantined with
a reason); a running total of real bugs and disclosed limitations found
while building this (not before) is documented per-question (Q4 §3, Q5
§3-4, Q6 §3, Q7 §2/§4, Q8 §7 — five separate bugs in Q8 alone, Q9 §3 — two
more, including a data-maturity artifact that would have silently reported
a fake -41% revenue collapse, Q10 §9 — a genuine architectural gap around
late historical corrections, disclosed rather than hidden), alongside
genuine, quantified findings that aren't bugs — just discovered facts about
the data (Q4 §4, Q5 §3, Q6 §3 Bug 1/2, Q7 §4, Q8 §1's corrected hierarchy,
Q9 §2 Q5's "zero campaigns exceed 10% of revenue").

### Local dev environment

A Docker Postgres container (`pulsetrack_dev`, two databases:
`pulsetrack_raw` for the simulated OLTP source, `pulsetrack_dw` for
bronze/silver/gold/mart/olap/audit/quality) was used to build and verify
every question, Q1 through Q10 — the entire pipeline, Mart, OLAP cubes and
Q10's live scenario tests all ran against this same instance, not a sample.
It is left running between sessions to avoid re-loading ~774K source rows
each time; recreate it with `docker run -d --name pulsetrack_dev -e
POSTGRES_PASSWORD=<choose-a-password> -p 55500:5432 postgres:16` if it's ever
removed. The Python scripts read that same password from the
`PULSETRACK_DB_PASSWORD` environment variable (it is not stored in the repo), so
set it before running anything in `python/`. Then
re-run the DDL/load scripts in `sql/` in numeric order per subfolder
(source schema + CSV `\copy` first, per the bootcamp repo's own
`ddl/pulsetrack_source_schema.sql` instructions), or run
`python/run_pipeline.py` for the Silver→Gold portion once Bronze is loaded.

### Software Versions Used

| Software | Version | Purpose |
|---|---|---|
| PostgreSQL | 16 | Sole engine for Bronze/Silver/Gold/Mart/OLAP — no separate warehouse or OLAP engine introduced (Q3's decision) |
| Docker | any recent | Runs the single `pulsetrack_dev` Postgres instance used for every question |
| Python | 3.13 (`py` launcher) | Extraction scripts, pipeline driver, audit helpers — `psycopg2` for all DB access |
| Apache Airflow | 3.x TaskFlow API (syntax-checked only) | `dags/pulsetrack_medallion_pipeline.py` — the orchestration design from Q3; never executed against a live Airflow instance, so its behaviour is verified by inspection/syntax, not by a real DAG run |

Q3's full technology evaluation (≥3 approaches scored against 14 criteria)
is in the report; only PostgreSQL, Docker and Python were actually run.
