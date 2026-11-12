"""Extract the 11 relational tables from pulsetrack_raw into Bronze.

Q4 scope note (a deliberate boundary, not an oversight): this is the INITIAL
FULL LOAD — every row, every run. True watermark-filtered incremental
extraction (only rows changed since last run) is Q5's remit, not Q4's, and
for a good reason specific to this dataset: created_at/updated_at are messy,
multi-format TEXT in the source (by design — see the source DDL's own
comments), so a server-side `WHERE updated_at > :watermark` cannot be pushed
down without first parsing every row anyway. Q5 addresses that properly
(with recovery/idempotency in scope at the same time); doing it half-way
here would just be wrong twice. What this script DOES do already: compute
and persist the watermark each table would need for that future incremental
run (via parsing.max_watermark), so Q5 has a real starting point rather than
building the watermark mechanism from nothing.
"""

import sys
import uuid
from datetime import datetime

import psycopg2.extras

from config import SOURCE_DB, WAREHOUSE_DB
from db import get_connection, dict_cursor
import audit
from parsing import max_watermark

BATCH_SIZE = 5000

PIPELINE_ID = "pulsetrack_medallion_pipeline"

TABLES = {
    "account_managers_raw": ["am_id", "am_name", "team", "region", "hire_date", "created_at", "updated_at"],
    "advertisers_raw": ["advertiser_id", "advertiser_name", "vertical", "account_manager_id",
                        "status", "signup_date", "billing_country", "created_at", "updated_at"],
    "partners_raw": ["partner_id", "partner_name", "channel", "media_buyer", "join_date",
                     "payment_terms", "created_at", "updated_at"],
    "offers_raw": ["offer_id", "offer_name", "advertiser_id", "vertical", "default_payout",
                   "payout_type", "created_at", "updated_at"],
    "campaigns_raw": ["campaign_id", "campaign_name", "offer_id", "advertiser_id", "price_type",
                      "start_date", "end_date", "created_at", "updated_at"],
    "creatives_raw": ["creative_id", "campaign_id", "creative_name", "format", "size",
                      "landing_url", "created_at", "updated_at"],
    "impressions_raw": ["impression_id", "campaign_id", "creative_id", "partner_id",
                        "impression_time", "placement_raw", "country", "device", "bot_score",
                        "created_at", "updated_at"],
    "clicks_raw": ["click_id", "impression_id", "campaign_id", "creative_id", "partner_id",
                   "click_time", "ip", "user_agent", "country", "device", "is_unique",
                   "referrer_raw", "created_at", "updated_at"],
    "conversions_raw": ["conversion_id", "click_id", "campaign_id", "advertiser_name",
                        "account_manager", "campaign_name", "partner_id", "partner_name",
                        "offer_name", "price_type", "conversion_time", "revenue", "payout",
                        "currency", "status", "country", "device", "sub_id", "is_test",
                        "legacy_flag", "pixel_fired", "internal_note", "reserved_1",
                        "created_at", "updated_at"],
    "conversion_touchpoints_raw": ["touchpoint_id", "conversion_id", "click_id", "touch_position",
                                   "weight", "created_at", "updated_at"],
    "revenue_adjustments_raw": ["adjustment_id", "conversion_id", "delta_amount", "reason",
                                "adjusted_at", "created_at", "updated_at"],
}


def extract_table(table_name, columns, batch_id, run_id):
    run_log_key = audit.start_step(
        WAREHOUSE_DB, batch_id, PIPELINE_ID, run_id,
        step_name=f"extract_{table_name}", layer="bronze", target_table=f"bronze.{table_name}",
    )
    src_conn = get_connection(SOURCE_DB)
    dw_conn = get_connection(WAREHOUSE_DB)
    try:
        col_list = ", ".join(columns)
        with dict_cursor(src_conn) as src_cur:
            src_cur.execute(f"SELECT {col_list} FROM pulsetrack_raw.{table_name}")
            rows = src_cur.fetchall()

        insert_cols = columns + ["batch_id"]
        insert_sql = f"INSERT INTO bronze.{table_name} ({', '.join(insert_cols)}) VALUES %s"

        watermark_inputs = []
        with dw_conn.cursor() as dw_cur:
            batch = []
            for row in rows:
                batch.append(tuple(row[c] for c in columns) + (batch_id,))
                if "created_at" in row:
                    watermark_inputs.append(row.get("created_at"))
                if "updated_at" in row:
                    watermark_inputs.append(row.get("updated_at"))
                if len(batch) >= BATCH_SIZE:
                    psycopg2.extras.execute_values(dw_cur, insert_sql, batch, page_size=BATCH_SIZE)
                    batch = []
            if batch:
                psycopg2.extras.execute_values(dw_cur, insert_sql, batch, page_size=BATCH_SIZE)
        dw_conn.commit()

        wm = max_watermark(*watermark_inputs) if watermark_inputs else None
        with dw_conn.cursor() as dw_cur:
            dw_cur.execute(
                """
                INSERT INTO audit.extraction_watermark (source_table, last_watermark, last_batch_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_table) DO UPDATE
                    SET last_watermark = EXCLUDED.last_watermark,
                        last_batch_id = EXCLUDED.last_batch_id,
                        updated_at = CURRENT_TIMESTAMP
                """,
                (table_name, wm, batch_id),
            )
        dw_conn.commit()

        audit.end_step(WAREHOUSE_DB, run_log_key, "success",
                        processed_count=len(rows), inserted_count=len(rows),
                        notes=f"watermark={wm}")
        print(f"  {table_name}: {len(rows)} rows -> bronze (watermark={wm})")
        return len(rows)
    except Exception as e:
        dw_conn.rollback()
        audit.end_step(WAREHOUSE_DB, run_log_key, "failed", error_count=1, notes=str(e))
        raise
    finally:
        src_conn.close()
        dw_conn.close()


def main():
    batch_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    run_id = str(uuid.uuid4())
    print(f"batch_id={batch_id} run_id={run_id}")
    total = 0
    for table_name, columns in TABLES.items():
        total += extract_table(table_name, columns, batch_id, run_id)
    print(f"TOTAL: {total} rows extracted to bronze")


if __name__ == "__main__":
    sys.exit(main())
