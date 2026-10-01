# Screenshots

Captured live against this project's own running stack — the actual Docker
Airflow containers (`miniproject2-airflow-*`) plus the native PostgreSQL 18.4
instance at `C:\pgsql` (the same instance Mini Project 01 uses) — via a
headless-browser automation script, not staged/mocked images.

**A real issue found and fixed while capturing these**: the native PostgreSQL
instance had been stopped (likely since a machine restart), which meant the
DAG's `postgres_analytical` connection (`host.docker.internal:5432`) had been
failing for at least 5 days of scheduled runs (2026-09-09 through 2026-09-13),
cascading from `log_audit_start` through every downstream task. This was **not
a code bug** — `pg_ctl start -D C:\pgsql\data` brought it back immediately and
the DAG connected on the next attempt — but it's the reason `10_task_retry.png`
shows 3 real failed attempts before a real success on try 4, and why
`02b_dags_list_history.png`/`02c_dag_runs_history_list.png` show a run of red
failures immediately preceding today's green success. Left as genuine evidence
rather than re-captured to look cleaner.

- [x] `01_dag_graph_view.png` — Graph view of `abc_hub_medallion_pipeline` for
      the fresh successful run (`scheduled__2026-09-13T00:00:00+00:00`),
      showing every TaskGroup (Extract, Data Validation, Silver
      Transformation, SCD Processing, Gold Load, Data Quality Validation) and
      their dependencies, all green.
- [x] `02_dag_successful_run.png` — That same run's Task Instances list: all
      30 tasks `Success`, with real operator types
      (`SQLExecuteQueryOperator`/`SQLCheckOperator`/`EmptyOperator`) and
      durations.
- [x] `02b_dags_list_history.png` — The Dags list showing the latest run
      green, with the run-history bar showing the preceding string of real
      failures before the fix (see above).
- [x] `02c_dag_runs_history_list.png` — The Runs tab's full history table.
- [x] `03_dag_run_configuration.png` — The **Rendered Templates** tab for
      `extract.extract_events`, showing `{{ data_interval_start }}`/
      `{{ data_interval_end }}`/`{{ run_id }}` already substituted with real
      values (`2026-09-12 00:00:00+00:00`, `2026-09-13 00:00:00+00:00`,
      `scheduled__2026-09-13T00:00:00+00:00`) in the actual SQL Airflow ran.
- [x] `04_postgres_query_results.png` — Real `psql` output against
      `abc_hub_analytical`, captured right after the fresh run: the audit log
      (top row is the fresh run, `inserted=6`/`updated=398382` repeating
      identically across prior runs — the idempotency signature), Gold layer
      row counts, a real `dim_customer` SCD2 version pair, and the
      `silver.dq_rejected` breakdown by reason.
- [x] `10_task_retry.png` — `log_audit_start`'s own Task Tries panel: attempts
      1-3 failed (`OperationalError`, the Postgres-down issue above), attempt
      4 succeeded — genuine retry/recovery evidence, not staged.
- [x] `09_backfill_cli.png` — Real `airflow backfill create --dag-id
      abc_hub_medallion_pipeline --from-date 2026-01-20 --to-date 2026-01-21
      --reprocess-behavior completed` invocation (forcing genuine
      re-execution of two already-completed logical dates), its actual
      terminal output, the resulting `SUCCESS` audit-log rows, and two direct
      duplicate-key checks (`fact_customer_daily_activity` grain,
      `dim_customer.is_current` per customer) both returning zero rows.
- [x] `09b_backfills_tab.png` — The Airflow UI's own Backfills tab, showing
      both real backfills on record: the original 2026-01-15 one and this
      new 2026-01-20→21 one, each with a real `Completed at` timestamp.

`airflow_cli_outputs.txt` / `postgres_query_outputs.txt` / `screenshot_queries.sql`
are kept as-is — real captured text evidence from an earlier session,
supplementary to the images above, not a substitute for them anymore.

## A second real bug found and fixed after these screenshots were captured

While pulling a real `dim_customer` SCD2 sample for the report (Q2 Part B),
every existing multi-version customer showed the same anomaly: the closed
version's `expiry_date` was dated *before* its own `effective_date` — logically
inverted. Root cause: `sql/gold/02_load_dimensions.sql`'s three SCD2 procedures
(`sp_load_dim_customer`/`sp_load_dim_content`/`sp_load_dim_inventory`) stamped
`effective_date`/`expiry_date` from `CURRENT_DATE` (real wall-clock time)
instead of the DAG run's own logical/business date — invisible on a single
call, but under `catchup=True` a fast scheduler pass replays many historical
logical dates within one real-time session, collapsing all of them onto
whatever day the pass actually executed on.

**Fixed**: each procedure now takes a `p_logical_date DATE` parameter, sourced
from the DAG's own `{{ ds }}` (see `dags/abc_hub_medallion_pipeline.py`'s
`scd_processing` TaskGroup) instead of `CURRENT_DATE`. Verified directly: forced
a real attribute change and called the fixed procedure with a deliberately
distinct test date — the new version's `effective_date` matched the supplied
date exactly, not the real day the call was made. A permanent, real
demonstration of this (customer 215, "Danny Montgomery", a genuine
Colombo → Kandy change) is now in `gold.dim_customer` and documented in the
report's Q2 Part B with the real `customer_key`/dates.

Rows created **before** this fix keep their (wrong) collapsed dates —
`bronze`/`silver` only retain each entity's latest state, not a full
point-in-time history, so there is no way to recover what those earlier
transitions' true effective dates should have been without inventing them.
Disclosed here rather than silently patched over.

## Two more things found during final verification, both fixed

**1. `CREATE OR REPLACE PROCEDURE` doesn't replace across a changed signature.**
Adding `p_logical_date` created a *second*, additional overload of each of the
three procedures rather than replacing the old one — the original one-argument
version (still containing the `CURRENT_DATE` bug) was left fully callable
alongside the fixed version. Caught by listing `pg_proc` directly rather than
assuming the fix was complete. Fixed by dropping the stale one-argument (and,
for `sp_load_dim_customer`/`content`, an even older zero-argument) overloads in
the `gold` schema, leaving exactly one version of each procedure - the fixed
one. (A same-named zero-argument version also exists in the separate `staging`
schema - that's Mini Project 01's own, unrelated procedure sharing this
database; left untouched, out of scope.)

**2. The Colombo → Kandy demo customer (originally 145) got overwritten by a
later test.** Running the Question 4 Part B backfill (above) for 2026-01-20/21
caused `silver_transformation.transform_customer` to re-derive `silver.customer`
from the real operational source for *every* customer touched that day,
including 145 - reverting the manually-patched `city_name` back to its real
value (`Colombo`) and triggering a second, correctly-dated SCD2 transition
(proving the fix works for a second, independent trigger, which is a genuine
positive - but leaving customer 145 with a three-version history that's
confusing to present as "the" example, since the transitions aren't in
chronological effective-date order). Fixed by re-demonstrating cleanly on a
different, untouched customer (215, "Danny Montgomery") and updating the report
to reference that one instead. Customer 145's own three-row history is real and
internally consistent (exactly one `is_current` row, as it should be) - it's
just no longer the report's chosen illustration.
