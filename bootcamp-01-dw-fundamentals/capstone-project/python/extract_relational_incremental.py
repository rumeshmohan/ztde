"""TRUE incremental extraction — the Q5 follow-on to Q4's full-load script.

Q4 deliberately deferred this: pulsetrack_raw's created_at/updated_at are
messy multi-format TEXT, so a server-side `WHERE updated_at > :watermark`
can't be pushed down without parsing every row first. The fix: fetch each
table (a full scan is unavoidable given the source can't filter on its own
messy columns — a source-side constraint, not a shortcut taken here), parse
each row's watermark in Python (parsing.py, already validated in Q1/Q4), and
only forward rows whose parsed watermark is STRICTLY newer than the
persisted watermark. Rows already seen (same or older watermark) are
skipped before they ever reach Bronze.

This single mechanism satisfies two of Q5's required scenarios at once:
  - "new/changed records" — a row with a newer updated_at is forwarded.
  - "duplicate delivery" — re-extracting rows already at or below the
    stored watermark forwards ZERO of them, so redelivering the same data
    (even the exact same 50,000 rows a second time) adds nothing.
"""

import sys
import uuid
from datetime import datetime

import psycopg2.extras

from config import SOURCE_DB, WAREHOUSE_DB
from db import get_connection, dict_cursor
import audit
from parsing import parse_dt, max_watermark
from extract_relational import TABLES, PIPELINE_ID, BATCH_SIZE


def get_last_watermark(table_name):
    conn = get_connection(WAREHOUSE_DB)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_watermark FROM audit.extraction_watermark WHERE source_table = %s",
                (table_name,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def extract_table_incremental(table_name, columns, batch_id, run_id):
    last_watermark = get_last_watermark(table_name)
    run_log_key = audit.start_step(
        WAREHOUSE_DB, batch_id, PIPELINE_ID, run_id,
        step_name=f"extract_incremental_{table_name}", layer="bronze",
        target_table=f"bronze.{table_name}",
    )
    src_conn = get_connection(SOURCE_DB)
    dw_conn = get_connection(WAREHOUSE_DB)
    try:
        col_list = ", ".join(columns)
        with dict_cursor(src_conn) as src_cur:
            src_cur.execute(f"SELECT {col_list} FROM pulsetrack_raw.{table_name}")
            all_rows = src_cur.fetchall()

        new_rows = []
        watermark_inputs = []
        for row in all_rows:
            row_wm = max_watermark(row.get("created_at"), row.get("updated_at"))
            if row_wm is None:
                continue  # can't tell if it's new; Silver's own quarantine already covers unparseable dates
            if last_watermark is not None and row_wm <= last_watermark:
                continue  # already seen this row at this (or a later) state — the duplicate-delivery guard
            new_rows.append(row)
            watermark_inputs.append(row_wm)

        if new_rows:
            insert_cols = columns + ["batch_id"]
            insert_sql = f"INSERT INTO bronze.{table_name} ({', '.join(insert_cols)}) VALUES %s"
            with dw_conn.cursor() as dw_cur:
                batch = [tuple(row[c] for c in columns) + (batch_id,) for row in new_rows]
                for i in range(0, len(batch), BATCH_SIZE):
                    psycopg2.extras.execute_values(dw_cur, insert_sql, batch[i:i + BATCH_SIZE], page_size=BATCH_SIZE)
            dw_conn.commit()

            new_watermark = max(watermark_inputs)
            with dw_conn.cursor() as dw_cur:
                dw_cur.execute(
                    """
                    INSERT INTO audit.extraction_watermark (source_table, last_watermark, last_batch_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (source_table) DO UPDATE
                        SET last_watermark = GREATEST(audit.extraction_watermark.last_watermark, EXCLUDED.last_watermark),
                            last_batch_id = EXCLUDED.last_batch_id, updated_at = CURRENT_TIMESTAMP
                    """,
                    (table_name, new_watermark, batch_id),
                )
            dw_conn.commit()

        audit.end_step(WAREHOUSE_DB, run_log_key, "success",
                        processed_count=len(all_rows), inserted_count=len(new_rows),
                        notes=f"prior_watermark={last_watermark}")
        print(f"  {table_name}: {len(all_rows)} scanned, {len(new_rows)} new/changed -> bronze "
              f"(prior watermark={last_watermark})")
        return len(new_rows)
    except Exception as e:
        dw_conn.rollback()
        audit.end_step(WAREHOUSE_DB, run_log_key, "failed", error_count=1, notes=str(e))
        raise
    finally:
        src_conn.close()
        dw_conn.close()


def main():
    batch_id = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    run_id = str(uuid.uuid4())
    print(f"batch_id={batch_id} run_id={run_id} (incremental mode)")
    total = 0
    for table_name, columns in TABLES.items():
        total += extract_table_incremental(table_name, columns, batch_id, run_id)
    print(f"TOTAL: {total} new/changed rows extracted to bronze")


if __name__ == "__main__":
    sys.exit(main())
