"""ABC Hub - Medallion (Bronze/Silver/Gold) pipeline, Mini Project 02.

Continuation of Mini Project 01's NiFi-based ETL: the same business
scenario, data model, and star schema, now orchestrated by Airflow
instead of hand-wired NiFi processors, with real Bronze/Silver/Gold
layering, SCD-aware idempotent loads, data-quality gates, and audit
logging.

Every extract/transform/load/check step below is a built-in Airflow
SQL operator running a .sql file (Jinja-rendered by Airflow via
`template_searchpath`, using real context variables -
`data_interval_start`/`data_interval_end`/`run_id` - not a hardcoded
date or a Python string-replace). The ONLY custom Python in this
whole DAG is `mark_run_failed` below: Airflow's failure-callback
mechanism is a Python callable by framework design, so there is no
SQL/operator alternative - see the report, Question 3 Part C, for
why every other step deliberately avoids Python.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.timetables.interval import CronDataIntervalTimetable

try:  # Airflow 3.x
    from airflow.sdk import DAG, TaskGroup
except ImportError:  # Airflow 2.x
    from airflow.models.dag import DAG
    from airflow.utils.task_group import TaskGroup

try:  # Airflow 3.x (apache-airflow-providers-standard)
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.empty import EmptyOperator

SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql")
CONN_ID = "postgres_analytical"  # the only Airflow connection this DAG needs -
                                  # postgres_fdw (sql/bronze/00_setup_fdw.sql)
                                  # makes the operational DB reachable from here.


def mark_run_failed(context):
    """DAG-level on_failure_callback - the one justified custom-Python
    step in this pipeline (see module docstring)."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(
        "UPDATE gold.etl_audit_log SET status = 'FAILED', end_time = CURRENT_TIMESTAMP WHERE run_id = %s;",
        parameters=(context["run_id"],),
    )


default_args = {
    "owner": "abc_hub_data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": mark_run_failed,
}

with DAG(
    dag_id="abc_hub_medallion_pipeline",
    description="ABC Hub Bronze/Silver/Gold pipeline with SCD, DQ gates, and audit logging",
    default_args=default_args,
    # A bare "@daily" string resolves to CronTriggerTimetable in Airflow 3.x -
    # point-in-time trigger semantics where data_interval_start ==
    # data_interval_end (Airflow 3's new default for plain cron strings, matching
    # literal crontab behaviour). This DAG's incremental extraction genuinely
    # needs a data INTERVAL (a full day's window), so the interval-based
    # timetable is requested explicitly instead.
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,          # required for `airflow dags backfill` (Question 4 Part B)
    max_active_runs=1,     # one batch at a time - simplifies watermark reasoning
    template_searchpath=[SQL_DIR],
    tags=["abc_hub", "medallion", "mini_project_02"],
) as dag:

    start = EmptyOperator(task_id="start")

    log_audit_start = SQLExecuteQueryOperator(
        task_id="log_audit_start",
        conn_id=CONN_ID,
        sql="audit/log_audit_start.sql",
    )

    # ------------------------------------------------------------
    # EXTRACT  (+ Bronze Load - postgres_fdw makes extraction and the
    # upsert-into-bronze a single statement per source, see
    # sql/bronze/00_setup_fdw.sql, so there is no separate "load" step
    # to schedule after extraction the way NiFi needed one)
    # ------------------------------------------------------------
    with TaskGroup(group_id="extract") as extract_group:
        extract_reference = SQLExecuteQueryOperator(
            task_id="extract_reference", conn_id=CONN_ID,
            sql="bronze/extract/01_extract_reference.sql", split_statements=True,
        )
        extract_customer = SQLExecuteQueryOperator(
            task_id="extract_customer", conn_id=CONN_ID,
            sql="bronze/extract/02_extract_customer.sql", split_statements=True,
        )
        extract_content = SQLExecuteQueryOperator(
            task_id="extract_content", conn_id=CONN_ID,
            sql="bronze/extract/03_extract_content.sql", split_statements=True,
        )
        extract_inventory = SQLExecuteQueryOperator(
            task_id="extract_inventory", conn_id=CONN_ID,
            sql="bronze/extract/04_extract_inventory.sql", split_statements=True,
        )
        extract_events = SQLExecuteQueryOperator(
            task_id="extract_events", conn_id=CONN_ID,
            sql="bronze/extract/05_extract_events.sql", split_statements=True,
        )

    bronze_load_complete = EmptyOperator(task_id="bronze_load_complete")

    # ------------------------------------------------------------
    # DATA VALIDATION - runs on Bronze, BEFORE Silver Transformation.
    # quarantine_bad_rows moves every violating row into
    # silver.dq_rejected; the six checks then assert quarantine
    # caught 100% of them. A check failing here means a bad row could
    # otherwise reach Silver - the task fails visibly rather than
    # letting that happen silently.
    # ------------------------------------------------------------
    with TaskGroup(group_id="data_validation") as data_validation_group:
        quarantine_bad_rows = SQLExecuteQueryOperator(
            task_id="quarantine_bad_rows", conn_id=CONN_ID,
            sql="quality/01_data_quality_checks.sql", split_statements=True,
        )
        check_mandatory_fields = SQLCheckOperator(
            task_id="check_mandatory_fields", conn_id=CONN_ID, sql="quality/checks/mandatory_fields.sql",
        )
        check_duplicates = SQLCheckOperator(
            task_id="check_duplicates", conn_id=CONN_ID, sql="quality/checks/duplicates.sql",
        )
        check_referential_integrity = SQLCheckOperator(
            task_id="check_referential_integrity", conn_id=CONN_ID, sql="quality/checks/referential_integrity.sql",
        )
        check_valid_dates = SQLCheckOperator(
            task_id="check_valid_dates", conn_id=CONN_ID, sql="quality/checks/valid_dates.sql",
        )
        check_valid_monetary = SQLCheckOperator(
            task_id="check_valid_monetary", conn_id=CONN_ID, sql="quality/checks/valid_monetary.sql",
        )
        check_valid_statuses = SQLCheckOperator(
            task_id="check_valid_statuses", conn_id=CONN_ID, sql="quality/checks/valid_statuses.sql",
        )
        quarantine_bad_rows >> [
            check_mandatory_fields, check_duplicates, check_referential_integrity,
            check_valid_dates, check_valid_monetary, check_valid_statuses,
        ]

    # ------------------------------------------------------------
    # SILVER TRANSFORMATION - cleansing, denormalising joins, and the
    # WHERE NOT(...) exclusions that mirror the quarantine rules above,
    # so a quarantined row can never also land in Silver.
    # ------------------------------------------------------------
    with TaskGroup(group_id="silver_transformation") as silver_transformation_group:
        transform_customer = SQLExecuteQueryOperator(
            task_id="transform_customer", conn_id=CONN_ID, sql="silver/02_transform_customer.sql",
        )
        transform_content = SQLExecuteQueryOperator(
            task_id="transform_content", conn_id=CONN_ID, sql="silver/03_transform_content.sql",
        )
        transform_reference = SQLExecuteQueryOperator(
            task_id="transform_reference", conn_id=CONN_ID,
            sql="silver/04_transform_reference.sql", split_statements=True,
        )
        transform_inventory = SQLExecuteQueryOperator(
            task_id="transform_inventory", conn_id=CONN_ID, sql="silver/05_transform_inventory.sql",
        )
        transform_events = SQLExecuteQueryOperator(
            task_id="transform_events", conn_id=CONN_ID,
            sql="silver/06_transform_events.sql", split_statements=True,
        )

    # ------------------------------------------------------------
    # SCD PROCESSING - strictly ordered (reference -> customer ->
    # content -> inventory) so every dimension is fully loaded before
    # Gold Load reads it. p_batch_id is passed as a real Jinja value,
    # not hardcoded - see sql/gold/02_load_dimensions.sql's header.
    #
    # The three SCD2 procedures also take p_logical_date ({{ ds }} -
    # the run's own business date, i.e. data_interval_start) so that
    # effective_date/expiry_date reflect the date this run actually
    # represents, not the real wall-clock day the task happens to
    # execute on - a real bug found by inspecting dim_customer's own
    # data (see that procedure's header for the full story) where a
    # fast catchup collapsed many historical transitions onto a
    # single real day.
    # ------------------------------------------------------------
    with TaskGroup(group_id="scd_processing") as scd_processing_group:
        load_dim_reference = SQLExecuteQueryOperator(
            task_id="load_dim_reference", conn_id=CONN_ID,
            sql="CALL gold.sp_load_dim_reference('{{ run_id }}');",
        )
        load_dim_customer = SQLExecuteQueryOperator(
            task_id="load_dim_customer", conn_id=CONN_ID,
            sql="CALL gold.sp_load_dim_customer('{{ run_id }}', '{{ ds }}');",
        )
        load_dim_content = SQLExecuteQueryOperator(
            task_id="load_dim_content", conn_id=CONN_ID,
            sql="CALL gold.sp_load_dim_content('{{ run_id }}', '{{ ds }}');",
        )
        load_dim_inventory = SQLExecuteQueryOperator(
            task_id="load_dim_inventory", conn_id=CONN_ID,
            sql="CALL gold.sp_load_dim_inventory('{{ run_id }}', '{{ ds }}');",
        )
        load_dim_reference >> load_dim_customer >> load_dim_content >> load_dim_inventory

    # ------------------------------------------------------------
    # GOLD LOAD - all three facts depend on every SCD step above, so a
    # fact row can always resolve the surrogate key it needs.
    # ------------------------------------------------------------
    with TaskGroup(group_id="gold_load") as gold_load_group:
        load_fact_customer_daily = SQLExecuteQueryOperator(
            task_id="load_fact_customer_daily", conn_id=CONN_ID,
            sql="CALL gold.sp_load_fact_customer_daily('{{ run_id }}');",
        )
        load_fact_content_monthly = SQLExecuteQueryOperator(
            task_id="load_fact_content_monthly", conn_id=CONN_ID,
            sql="CALL gold.sp_load_fact_content_monthly('{{ run_id }}');",
        )
        load_fact_inventory_util = SQLExecuteQueryOperator(
            task_id="load_fact_inventory_util", conn_id=CONN_ID,
            sql="CALL gold.sp_load_fact_inventory_util('{{ run_id }}');",
        )

    # ------------------------------------------------------------
    # DATA QUALITY VALIDATION - post-Gold-Load regression check that
    # every fact row resolved a real dimension member (Question 4 Part C).
    # ------------------------------------------------------------
    with TaskGroup(group_id="data_quality_validation") as data_quality_validation_group:
        check_fact_dim_consistency = SQLCheckOperator(
            task_id="check_fact_dim_consistency", conn_id=CONN_ID,
            sql="quality/checks/fact_dim_consistency.sql",
        )

    log_audit_end = SQLExecuteQueryOperator(
        task_id="log_audit_end", conn_id=CONN_ID,
        sql="audit/log_audit_end.sql",
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> log_audit_start
        >> extract_group
        >> bronze_load_complete
        >> data_validation_group
        >> silver_transformation_group
        >> scd_processing_group
        >> gold_load_group
        >> data_quality_validation_group
        >> log_audit_end
        >> finish
    )
