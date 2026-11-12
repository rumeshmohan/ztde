"""Parse the two MongoDB JSONL export files directly into Bronze as JSONB.

No live MongoDB instance is used: the .jsonl files ARE the export a real
extraction from MongoDB would produce (the data guide's own framing —
"mongoexport-style"), so parsing them directly is extracting from Mongo, not
a shortcut around it. Postgres's native JSONB is the landing format — this
is what "JSON ingestion" means in a Postgres-centric platform (Q3), not a
requirement to stand up a second database engine.

Malformed lines are quarantined, never silently skipped (Q3's error handling).
"""

import json
import sys
import uuid
from datetime import datetime

from config import WAREHOUSE_DB, MONGO_ADVERTISER_ACCOUNTS_PATH, MONGO_PARTNER_PROFILES_PATH
from db import get_connection
import audit

PIPELINE_ID = "pulsetrack_medallion_pipeline"


def load_jsonl(path, table_name, id_field, extra_id_field, batch_id, run_id):
    run_log_key = audit.start_step(
        WAREHOUSE_DB, batch_id, PIPELINE_ID, run_id,
        step_name=f"extract_{table_name}", layer="bronze", target_table=f"bronze.{table_name}",
    )
    conn = get_connection(WAREHOUSE_DB)
    inserted = 0
    rejected = 0
    processed = 0
    try:
        with open(path, "r", encoding="utf-8") as f, conn.cursor() as cur:
            for line in f:
                processed += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError as e:
                    rejected += 1
                    audit.quarantine(WAREHOUSE_DB, batch_id, table_name, None,
                                      f"malformed JSON: {e}", line)
                    continue

                mongo_id = (doc.get("_id") or {}).get("$oid")
                natural_id = doc.get(extra_id_field)

                cur.execute(
                    f"""
                    INSERT INTO bronze.{table_name} (mongo_id, {id_field}, raw_doc, batch_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (mongo_id, natural_id, json.dumps(doc), batch_id),
                )
                inserted += 1
        conn.commit()
        audit.end_step(WAREHOUSE_DB, run_log_key, "success",
                        processed_count=processed, inserted_count=inserted,
                        rejected_count=rejected)
        print(f"  {table_name}: {processed} lines, {inserted} loaded, {rejected} quarantined")
        return inserted, rejected
    except Exception as e:
        conn.rollback()
        audit.end_step(WAREHOUSE_DB, run_log_key, "failed", error_count=1, notes=str(e))
        raise
    finally:
        conn.close()


def main():
    batch_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    run_id = str(uuid.uuid4())
    print(f"batch_id={batch_id} run_id={run_id}")
    load_jsonl(MONGO_ADVERTISER_ACCOUNTS_PATH, "mongo_advertiser_accounts",
               "advertiser_id", "advertiser_id", batch_id, run_id)
    load_jsonl(MONGO_PARTNER_PROFILES_PATH, "mongo_partner_profiles",
               "partner_id", "partner_id", batch_id, run_id)


if __name__ == "__main__":
    sys.exit(main())
