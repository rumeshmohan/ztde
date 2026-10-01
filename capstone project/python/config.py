"""DB connection configs. Follows Week 03's config.py pattern (a plain dict
per target), extended to two databases: the simulated OLTP source
(pulsetrack_raw) and this project's own warehouse (pulsetrack_dw).

Real deployments would pull these from Airflow Connections (Q3's security
decision), not a committed file — local dev only.
"""

import os

# The MongoDB JSONL export and Kafka-style stream log are NOT duplicated into
# this project (matching miniproject1's convention: the official data
# package stays in the bootcamp repo, referenced by path). Adjust this if the
# two project folders are ever moved to a different relative layout.
_HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE_DATA_ROOT = os.path.normpath(
    os.path.join(_HERE, "..", "..", "dw-fundamentals-bootcamp",
                 "Z2D DWDF Capstone Project", "DE01Capstone")
)
MONGO_ADVERTISER_ACCOUNTS_PATH = os.path.join(SOURCE_DATA_ROOT, "mongo", "advertiser_accounts.jsonl")
MONGO_PARTNER_PROFILES_PATH = os.path.join(SOURCE_DATA_ROOT, "mongo", "partner_profiles.jsonl")
STREAM_LOG_PATH = os.path.join(SOURCE_DATA_ROOT, "stream", "pulsetrack_conversions.log")

# The password is read from the environment, not stored in the repo. Set it to the
# password you gave the Postgres container (see the README), e.g.
#   PowerShell:  $env:PULSETRACK_DB_PASSWORD = "..."
#   bash:        export PULSETRACK_DB_PASSWORD=...
_DB_PASSWORD = os.environ["PULSETRACK_DB_PASSWORD"]

SOURCE_DB = {
    "host": "localhost",
    "port": 55500,
    "dbname": "pulsetrack_raw",
    "user": "postgres",
    "password": _DB_PASSWORD,
}

WAREHOUSE_DB = {
    "host": "localhost",
    "port": 55500,
    "dbname": "pulsetrack_dw",
    "user": "postgres",
    "password": _DB_PASSWORD,
}
