# ztde: Zero to Data Engineer projects

My work from the Zuu Crew "Zero to Data Engineer" programme.

## Layout

```
ztde/
├── bootcamp-01-dw-fundamentals/     Bootcamp 01: Data Warehousing Fundamentals projects
├── bootcamp-02-big-data-pipelines/  Bootcamp 02: Big Data Pipelines, weekly lab notes
├── course-repos/                    Zuu Crew course repositories (cloned locally, not in this repo)
└── submissions/                     Zipped submissions sent to the course (local only, not in this repo)
```

## Bootcamp 01: Data Warehousing Fundamentals

| Folder | Project | Main tools |
|---|---|---|
| [`capstone-project`](bootcamp-01-dw-fundamentals/capstone-project) | PulseTrack: an end-to-end data warehouse for an ad-tech platform (Bronze, Silver, Gold, Data Mart, OLAP, business analytics) | PostgreSQL 16, Python, SQL, an Airflow DAG (syntax-checked only, not run on a live Airflow) |
| [`miniproject1`](bootcamp-01-dw-fundamentals/miniproject1) | ABC Hub: a NiFi ETL pipeline loading PostgreSQL star schemas | Apache NiFi 2.10, PostgreSQL 18.4 |
| [`miniproject2`](bootcamp-01-dw-fundamentals/miniproject2) | ABC Hub: an Airflow pipeline with Medallion layers, SCD Type 2, data-quality checks, audit logging and backfill | Apache Airflow 3.3, PostgreSQL 18.4, Docker Compose |

Each folder has its own README (structure, versions, setup, how to run) and a written report
in its `report/` folder.

## Bootcamp 02: Big Data Pipelines

Labs run on Cloudera QuickStart (CDH 5.7) in local Docker.
[`week01/cloudera-daily-start.txt`](bootcamp-02-big-data-pipelines/week01/cloudera-daily-start.txt)
has the start, stop and troubleshooting steps.

## What is not in this repo

- **The datasets.** They are supplied by the course. The capstone README says where its data
  lives in the bootcamp repository (`course-repos/dw-fundamentals-bootcamp`), and the ABC Hub
  projects use the course's ABC Hub Data Package.
- **Course repositories** under `course-repos/`. Clone them from Zuu Crew's GitHub.
- **Credentials.** None are stored. Passwords are read from environment variables or are
  placeholders you replace (see `bootcamp-01-dw-fundamentals/capstone-project/README.md` and
  `bootcamp-01-dw-fundamentals/miniproject2/.env.example`).
- **Submissions** under `submissions/`.
- **Local environments and run output** (virtual environments, Airflow logs, caches).
