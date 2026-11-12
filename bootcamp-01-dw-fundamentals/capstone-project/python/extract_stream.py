"""Parse the Kafka-style conversion event log directly into Bronze.

No live Kafka broker is used: the .log file IS a topic dump ("exactly as a
consumer of all 4 partitions would see it" — stream/README_topic.md), so
parsing it line-by-line in offset order is exactly what a real consumer does.

Per the stream's documented semantics (README_topic.md), this stage does
NOT deduplicate by event_id and does NOT resolve schema v2's field rename —
those are explicitly Silver's job (dedup/standardisation), matching the
medallion principle that Bronze preserves exactly what was on the wire,
duplicates and all. What this stage DOES do: parse the envelope, quarantine
truncated/malformed lines (~0.1%, by design) rather than let a parse
exception kill the whole batch — the documented producer-retry semantics
mean the intact record follows later on the same partition, so nothing is
lost by quarantining the partial write.
"""

import json
import re
import sys
import uuid
from datetime import datetime

import psycopg2.extras

from config import WAREHOUSE_DB, STREAM_LOG_PATH
from db import get_connection
import audit

PIPELINE_ID = "pulsetrack_medallion_pipeline"
BATCH_SIZE = 5000

_INSERT_SQL = """
    INSERT INTO bronze.stream_events
        (topic, partition, kafka_offset, envelope_ts, event_key,
         event_id, event_type, schema_version, payload, raw_line,
         is_malformed, batch_id)
    VALUES %s
"""

_LINE_RE = re.compile(
    r"^(?P<topic>[^|]+)\|partition=(?P<partition>\d+)\|offset=(?P<offset>\d+)"
    r"\|ts=(?P<ts>\d+)\|key=(?P<key>[^|]*)\|(?P<payload>.*)$"
)


def main():
    batch_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    run_id = str(uuid.uuid4())
    print(f"batch_id={batch_id} run_id={run_id}")

    run_log_key = audit.start_step(
        WAREHOUSE_DB, batch_id, PIPELINE_ID, run_id,
        step_name="extract_stream_events", layer="bronze", target_table="bronze.stream_events",
    )

    conn = get_connection(WAREHOUSE_DB)
    processed = 0
    inserted = 0
    malformed = 0
    batch = []

    def flush(cur):
        nonlocal batch
        if batch:
            psycopg2.extras.execute_values(cur, _INSERT_SQL, batch, page_size=BATCH_SIZE)
            batch = []

    try:
        with open(STREAM_LOG_PATH, "r", encoding="utf-8") as f, conn.cursor() as cur:
            for line in f:
                processed += 1
                raw_line = line.rstrip("\n")
                if not raw_line.strip():
                    continue

                m = _LINE_RE.match(raw_line)
                if not m:
                    malformed += 1
                    _insert_malformed(cur, batch_id, raw_line)
                    continue

                payload_text = m.group("payload")
                try:
                    payload = json.loads(payload_text)
                except json.JSONDecodeError:
                    malformed += 1
                    _insert_malformed(cur, batch_id, raw_line,
                                      topic=m.group("topic"), partition=int(m.group("partition")),
                                      offset=int(m.group("offset")), ts=int(m.group("ts")),
                                      key=m.group("key"))
                    continue

                event_id = payload.get("event_id")
                event_type = payload.get("event_type")
                schema_version = payload.get("schema_version", "1.0")

                batch.append((
                    m.group("topic"), int(m.group("partition")), int(m.group("offset")),
                    int(m.group("ts")), m.group("key"), event_id, event_type,
                    schema_version, json.dumps(payload), raw_line, False, batch_id,
                ))
                inserted += 1
                if len(batch) >= BATCH_SIZE:
                    flush(cur)
            flush(cur)
        conn.commit()
        audit.end_step(WAREHOUSE_DB, run_log_key, "success",
                        processed_count=processed, inserted_count=inserted,
                        rejected_count=malformed)
        print(f"  stream_events: {processed} lines, {inserted} loaded, {malformed} quarantined "
              f"({malformed/processed:.2%})")
    except Exception as e:
        conn.rollback()
        audit.end_step(WAREHOUSE_DB, run_log_key, "failed", error_count=1, notes=str(e))
        raise
    finally:
        conn.close()


def _insert_malformed(cur, batch_id, raw_line, topic=None, partition=None, offset=None, ts=None, key=None):
    cur.execute(
        """
        INSERT INTO bronze.stream_events
            (topic, partition, kafka_offset, envelope_ts, event_key, raw_line, is_malformed, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s)
        """,
        (topic, partition, offset, ts, key, raw_line, batch_id),
    )
    audit.quarantine(WAREHOUSE_DB, batch_id, "stream_events", key,
                      "malformed/truncated stream line", raw_line)


if __name__ == "__main__":
    sys.exit(main())
