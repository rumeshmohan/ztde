-- ============================================================
-- Task: log_audit_start (runs immediately after Start)
-- One row per DAG run in gold.etl_audit_log, keyed on run_id so a
-- manual re-run of the SAME run_id (e.g. `airflow tasks clear`)
-- updates the existing row instead of creating a second one -
-- idempotency applies to the audit trail too.
--
-- start_time/end_time are real wall-clock execution instants
-- (CURRENT_TIMESTAMP), not the run's simulated logical/business date
-- ({{ ts }}) - the batch_id/run_id columns already identify which
-- logical date this batch processed. Using {{ ts }} here would make
-- start_time an artificial past date while log_audit_end.sql's
-- end_time is real "now", producing a nonsensical duration under
-- catchup/backfill (many historical logical dates processed within
-- one real-time session) - exactly the bug this comment replaced.
-- ============================================================
INSERT INTO gold.etl_audit_log (batch_id, dag_id, run_id, start_time, status)
VALUES ('{{ run_id }}', '{{ dag.dag_id }}', '{{ run_id }}', CURRENT_TIMESTAMP, 'RUNNING')
ON CONFLICT (run_id) DO UPDATE SET
    start_time = EXCLUDED.start_time,
    status     = 'RUNNING',
    end_time   = NULL;
