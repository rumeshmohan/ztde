"""PulseTrack medallion pipeline — Airflow 3.x TaskFlow API (Q3's decision:
Python + SQL + Airflow, TaskFlow over classic operators per Week 10's more
current pattern).

Every task here calls the exact same scripts verified directly against
PostgreSQL during Q4/Q5's build (python/extract_*.py, sql/{silver,gold}/*.sql) —
this DAG is the orchestration wrapper around already-proven logic, not a
new, untested code path. Dependency structure follows Q2's conformed
dimension bus matrix: Gold facts wait for ALL their dimensions, not just
their own source table (Q3's "Dependency management" architecture note).

STEADY-STATE, not bootstrap (Q5): this DAG runs the INCREMENTAL scripts
(extract_relational_incremental.py, 11_incremental_load_dim_advertiser.sql)
that preserve existing surrogate keys and only touch new/changed rows.
09_load_bridge_and_adjustments.sql IS still run daily below — Q5 made its
INSERTs `ON CONFLICT DO NOTHING`, so on a steady-state day it safely
inserts only touchpoints/adjustments for conversions that are themselves
new, and does nothing for ones already loaded; it is not bootstrap-only.
10_apply_new_revenue_adjustments.sql runs directly after it, every day, for
the reason noted at 08_load_fact_conversion.sql's adjustment_delta_total
column: 09 records adjustment rows but never rolls the total up, so 10 is
not optional maintenance, it's a required step of every run. Only two
scripts really are bootstrap-only, invoked directly and not from this DAG,
when a genuine from-scratch rebuild is needed (Q5 §6's backfill case):
extract_relational.py's full-scan mode, and 05_load_dim_advertiser.sql
(which truncates and would destroy every surrogate key if run again).
"""

from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

WAREHOUSE_CONN_ID = "pulsetrack_dw"
SQL_ROOT = "/opt/airflow/sql"  # mounted path to this project's sql/ folder


def _run_sql_file(path):
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    with open(path, "r", encoding="utf-8") as f:
        hook.run(f.read())


@dag(
    dag_id="pulsetrack_medallion_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": 300},
    tags=["pulsetrack", "capstone"],
)
def pulsetrack_medallion_pipeline():

    @task
    def extract_relational():
        import subprocess
        subprocess.run(["python", "/opt/airflow/python/extract_relational_incremental.py"], check=True)

    @task
    def extract_mongo():
        import subprocess
        subprocess.run(["python", "/opt/airflow/python/extract_mongo.py"], check=True)

    @task
    def extract_stream():
        import subprocess
        subprocess.run(["python", "/opt/airflow/python/extract_stream.py"], check=True)

    @task
    def silver_reference_dims():
        _run_sql_file(f"{SQL_ROOT}/silver/01_load_reference_dims.sql")

    @task
    def silver_funnel():
        _run_sql_file(f"{SQL_ROOT}/silver/02_load_funnel.sql")

    @task
    def silver_conversions():
        _run_sql_file(f"{SQL_ROOT}/silver/03_load_conversions.sql")

    @task
    def silver_touchpoints_and_adjustments():
        _run_sql_file(f"{SQL_ROOT}/silver/04_load_touchpoints_and_adjustments.sql")

    @task
    def gold_dim_date():
        _run_sql_file(f"{SQL_ROOT}/gold/03_load_dim_date.sql")

    @task
    def gold_dim_account_manager():
        _run_sql_file(f"{SQL_ROOT}/gold/04_load_dim_account_manager.sql")

    @task
    def gold_dim_advertiser():
        _run_sql_file(f"{SQL_ROOT}/gold/11_incremental_load_dim_advertiser.sql")

    @task
    def gold_dim_campaign_creative_partner():
        _run_sql_file(f"{SQL_ROOT}/gold/06_load_dim_campaign_creative_partner.sql")

    @task
    def gold_fact_impression_click():
        _run_sql_file(f"{SQL_ROOT}/gold/07_load_fact_impression_click.sql")

    @task
    def gold_fact_conversion():
        _run_sql_file(f"{SQL_ROOT}/gold/08_load_fact_conversion.sql")

    @task
    def gold_bridge_and_adjustments():
        _run_sql_file(f"{SQL_ROOT}/gold/09_load_bridge_and_adjustments.sql")

    @task
    def gold_apply_new_revenue_adjustments():
        _run_sql_file(f"{SQL_ROOT}/gold/10_apply_new_revenue_adjustments.sql")

    # --- dependency graph (mirrors Q2 B.2's conformed dimension bus matrix) ---
    rel = extract_relational()
    mongo = extract_mongo()
    stream = extract_stream()

    ref_dims = silver_reference_dims()
    funnel = silver_funnel()
    conversions = silver_conversions()
    touchpoints = silver_touchpoints_and_adjustments()

    [rel, mongo] >> ref_dims
    [rel, ref_dims] >> funnel
    [ref_dims, funnel] >> conversions
    [conversions, stream] >> touchpoints

    d_date = gold_dim_date()
    d_am = gold_dim_account_manager()
    d_adv = gold_dim_advertiser()
    d_ccp = gold_dim_campaign_creative_partner()

    ref_dims >> [d_am, d_ccp]
    d_am >> d_adv

    f_ic = gold_fact_impression_click()
    [d_date, d_ccp, funnel] >> f_ic

    f_conv = gold_fact_conversion()
    [d_date, d_adv, d_ccp, conversions, f_ic] >> f_conv

    f_bridge = gold_bridge_and_adjustments()
    [f_conv, f_ic, touchpoints] >> f_bridge

    apply_adj = gold_apply_new_revenue_adjustments()
    f_bridge >> apply_adj


pulsetrack_medallion_pipeline()
