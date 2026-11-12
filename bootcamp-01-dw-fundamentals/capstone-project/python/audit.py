"""Helpers to write audit.pipeline_run_log entries (Q3's metadata/audit
cross-cutting concern; column set matches Q6's explicit requirement).
"""

from db import get_connection


def start_step(config, batch_id, pipeline_id, run_id, step_name, layer, target_table=None, source_count=None):
    conn = get_connection(config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.pipeline_run_log
                    (batch_id, pipeline_id, run_id, step_name, layer, target_table, status, source_count)
                VALUES (%s, %s, %s, %s, %s, %s, 'running', %s)
                RETURNING run_log_key
                """,
                (batch_id, pipeline_id, run_id, step_name, layer, target_table, source_count),
            )
            run_log_key = cur.fetchone()[0]
        conn.commit()
        return run_log_key
    finally:
        conn.close()


def end_step(config, run_log_key, status, processed_count=0, inserted_count=0,
             updated_count=0, rejected_count=0, error_count=0, notes=None):
    conn = get_connection(config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audit.pipeline_run_log
                SET status = %s, end_time = CURRENT_TIMESTAMP,
                    processed_count = %s, inserted_count = %s, updated_count = %s,
                    rejected_count = %s, error_count = %s, notes = %s
                WHERE run_log_key = %s
                """,
                (status, processed_count, inserted_count, updated_count,
                 rejected_count, error_count, notes, run_log_key),
            )
        conn.commit()
    finally:
        conn.close()


def quarantine(config, batch_id, source_table, source_pk, reason, raw_payload):
    conn = get_connection(config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.quarantine (batch_id, source_table, source_pk, reason, raw_payload)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (batch_id, source_table, source_pk, reason, raw_payload),
            )
        conn.commit()
    finally:
        conn.close()
