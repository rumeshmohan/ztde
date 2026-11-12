"""Full pipeline driver with real audit instrumentation (Q6).

Q4/Q5 built audit.pipeline_run_log with exactly the columns Q6 requires
(batch_id, pipeline_id, run_id, start/end time, status, source/processed/
inserted/updated/rejected/error counts) — but only the 3 Python extraction
scripts ever wrote to it. Every Silver/Gold SQL step ran with zero audit
trail. Rather than hand-editing DO blocks with GET DIAGNOSTICS into 13
separate .sql files (fragile, and CREATE TABLE AS — most of Silver's
pattern — doesn't support GET DIAGNOSTICS row counts the way plain
INSERT/UPDATE does), this wraps every SQL step centrally: measure the
target table's row count before, run the file, measure it after. The
mechanism is generic (works for a full CREATE TABLE AS rebuild and for an
incremental upsert alike) and is also exactly what the Airflow DAG's
_run_sql_file should be doing per task, in one place instead of thirteen.
"""

import sys
import uuid
from datetime import datetime

from config import WAREHOUSE_DB
from db import get_connection
import audit

PIPELINE_ID = "pulsetrack_medallion_pipeline"

# (step_name, layer, sql_path, target_table_for_count | None)
STEPS = [
    ("silver_reference_dims", "silver", "sql/silver/01_load_reference_dims.sql", "silver.campaigns"),
    ("silver_funnel", "silver", "sql/silver/02_load_funnel.sql", "silver.impressions"),
    ("silver_conversions", "silver", "sql/silver/03_load_conversions.sql", "silver.conversions"),
    ("silver_touchpoints_and_adjustments", "silver", "sql/silver/04_load_touchpoints_and_adjustments.sql", "silver.conversion_touchpoints"),
    ("gold_dim_date", "gold", "sql/gold/03_load_dim_date.sql", "gold.dim_date"),
    ("gold_dim_advertiser_incremental", "gold", "sql/gold/11_incremental_load_dim_advertiser.sql", "gold.dim_advertiser"),
    ("gold_fact_impression_click", "gold", "sql/gold/07_load_fact_impression_click.sql", "gold.fact_impression"),
    # same idempotent (ON CONFLICT DO NOTHING) script; run again purely to
    # get a second audit entry against its OTHER target table, fact_click —
    # this driver only tracks one target per step, and the script loads two
    ("gold_fact_click_audit", "gold", "sql/gold/07_load_fact_impression_click.sql", "gold.fact_click"),
    ("gold_fact_conversion", "gold", "sql/gold/08_load_fact_conversion.sql", "gold.fact_conversion"),
    ("gold_bridge_and_adjustments", "gold", "sql/gold/09_load_bridge_and_adjustments.sql", "gold.bridge_conversion_touchpoint"),
    ("gold_apply_new_revenue_adjustments", "gold", "sql/gold/10_apply_new_revenue_adjustments.sql", "gold.fact_revenue_adjustment"),
]

# Type-1 dimension upserts moved OUT of STEPS above: Q6 rewrote both to end
# with a RETURNING(xmax=0)-based SELECT that reports TRUE inserted/updated
# counts (the before/after net-delta approach above is blind to updates —
# a Type-1 upsert that only changes existing attribute values shows a net
# delta of 0 even when every row was refreshed). Handled separately by
# run_upsert_step, which fetches that result row instead of counting.
UPSERT_STEPS = [
    ("gold_dim_account_manager", "sql/gold/04_load_dim_account_manager.sql",
     [("gold.dim_account_manager", "inserted", "updated")]),
    ("gold_dim_campaign_creative_partner", "sql/gold/06_load_dim_campaign_creative_partner.sql",
     [("gold.dim_campaign", "campaign_inserted", "campaign_updated"),
      ("gold.dim_creative", "creative_inserted", "creative_updated"),
      ("gold.dim_partner", "partner_inserted", "partner_updated")]),
]


def table_count(conn, qualified_table):
    schema, table = qualified_table.split(".")
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        return cur.fetchone()[0]


def quarantine_count(conn, batch_id_prefix=None):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM audit.quarantine")
        return cur.fetchone()[0]


def run_step(step_name, layer, sql_path, target_table, batch_id, run_id):
    conn = get_connection(WAREHOUSE_DB)
    try:
        before_rows = table_count(conn, target_table) if target_table else None
        before_quarantine = quarantine_count(conn)
    finally:
        conn.close()

    run_log_key = audit.start_step(
        WAREHOUSE_DB, batch_id, PIPELINE_ID, run_id,
        step_name=step_name, layer=layer, target_table=target_table,
        source_count=before_rows,
    )
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_text = f.read()
        conn = get_connection(WAREHOUSE_DB)
        try:
            with conn.cursor() as cur:
                cur.execute(sql_text)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        conn = get_connection(WAREHOUSE_DB)
        try:
            after_rows = table_count(conn, target_table) if target_table else None
            after_quarantine = quarantine_count(conn)
        finally:
            conn.close()

        inserted = (after_rows - before_rows) if (before_rows is not None and after_rows is not None) else None
        rejected_delta = after_quarantine - before_quarantine

        audit.end_step(
            WAREHOUSE_DB, run_log_key, "success",
            processed_count=after_rows, inserted_count=inserted,
            rejected_count=rejected_delta,
            notes=f"{target_table}: {before_rows} -> {after_rows} rows; quarantine +{rejected_delta}",
        )
        print(f"  {step_name}: {target_table} {before_rows} -> {after_rows} "
              f"(+{inserted if inserted is not None else '?'}), quarantine +{rejected_delta}")
    except Exception as e:
        audit.end_step(WAREHOUSE_DB, run_log_key, "failed", error_count=1, notes=str(e))
        print(f"  {step_name}: FAILED - {e}")
        raise


def run_upsert_step(step_name, sql_path, targets, batch_id, run_id):
    """targets: [(qualified_table, inserted_col, updated_col), ...] matching
    the column names the script's final SELECT returns, one triple per
    dimension the script upserts."""
    conn = get_connection(WAREHOUSE_DB)
    try:
        before_quarantine = quarantine_count(conn)
    finally:
        conn.close()

    run_log_keys = {}
    for target_table, _, _ in targets:
        run_log_keys[target_table] = audit.start_step(
            WAREHOUSE_DB, batch_id, PIPELINE_ID, run_id,
            step_name=f"{step_name}__{target_table.split('.')[-1]}", layer="gold",
            target_table=target_table,
        )

    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_text = f.read()
        conn = get_connection(WAREHOUSE_DB)
        try:
            with conn.cursor() as cur:
                cur.execute(sql_text)
                row = cur.fetchone()
                columns = [d.name for d in cur.description]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        result = dict(zip(columns, row))

        conn = get_connection(WAREHOUSE_DB)
        try:
            after_quarantine = quarantine_count(conn)
        finally:
            conn.close()
        rejected_delta = after_quarantine - before_quarantine

        for target_table, inserted_col, updated_col in targets:
            inserted = result[inserted_col]
            updated = result[updated_col]
            audit.end_step(
                WAREHOUSE_DB, run_log_keys[target_table], "success",
                processed_count=inserted + updated, inserted_count=inserted,
                updated_count=updated, rejected_count=rejected_delta,
                notes=f"{target_table}: {inserted} inserted, {updated} updated (xmax-based, real)",
            )
            print(f"  {step_name} -> {target_table}: {inserted} inserted, {updated} updated")
            rejected_delta = 0  # only attribute the quarantine delta to the first target once
    except Exception as e:
        for key in run_log_keys.values():
            audit.end_step(WAREHOUSE_DB, key, "failed", error_count=1, notes=str(e))
        print(f"  {step_name}: FAILED - {e}")
        raise


def main():
    batch_id = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    run_id = str(uuid.uuid4())
    print(f"batch_id={batch_id} run_id={run_id}")
    for step_name, layer, sql_path, target_table in STEPS:
        run_step(step_name, layer, sql_path, target_table, batch_id, run_id)
    for step_name, sql_path, targets in UPSERT_STEPS:
        run_upsert_step(step_name, sql_path, targets, batch_id, run_id)
    print("Pipeline run complete.")


if __name__ == "__main__":
    sys.exit(main())
