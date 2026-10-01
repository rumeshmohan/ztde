# ztde: Zero to Data Engineer projects

Three data warehousing projects from the Zuu Crew "Zero to Data Engineer" bootcamp.

| Folder | Project | Main tools |
|---|---|---|
| [`capstone project`](capstone%20project) | PulseTrack: an end-to-end data warehouse for an ad-tech platform (Bronze, Silver, Gold, Data Mart, OLAP, business analytics) | PostgreSQL 16, Python, SQL, an Airflow DAG (syntax-checked only, not run on a live Airflow) |
| [`miniproject1`](miniproject1) | ABC Hub: a NiFi ETL pipeline loading PostgreSQL star schemas | Apache NiFi 2.10, PostgreSQL 18.4 |
| [`miniproject2`](miniproject2) | ABC Hub: an Airflow pipeline with Medallion layers, SCD Type 2, data-quality checks, audit logging and backfill | Apache Airflow 3.3, PostgreSQL 18.4, Docker Compose |

Each folder has its own README (structure, versions, setup, how to run) and a written report
in its `report/` folder.

## What is not in this repo

- **The datasets.** They are supplied by the course. The capstone README says where its data
  lives in the bootcamp repository, and the ABC Hub projects use the course's ABC Hub Data Package.
- **Credentials.** None are stored. Passwords are read from environment variables or are
  placeholders you replace (see `capstone project/README.md` and `miniproject2/.env.example`).
- **Local environments and run output** (virtual environments, Airflow logs, caches).
