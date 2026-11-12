"""Q10 scenario test: 'the ETL process fails halfway through — after loading
dimensions but before completing fact loading.' Runs ONLY the dimension
steps (silver reference dims, gold dim_date, gold dim_advertiser, and the
UPSERT_STEPS dimension scripts), then stops — simulating a crash before any
fact-loading step runs. The full pipeline is run separately afterward to
prove the standard recovery procedure (just restart from step 1) is safe.
"""
import sys
sys.path.insert(0, ".")
from datetime import datetime
import uuid
import run_pipeline as rp

batch_id = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + "-PARTIAL-SIM"
run_id = str(uuid.uuid4())
print(f"[PARTIAL RUN] batch_id={batch_id} run_id={run_id}")

DIMENSION_STEPS = [s for s in rp.STEPS if s[0] in (
    "silver_reference_dims", "gold_dim_date", "gold_dim_advertiser_incremental",
)]
print(f"Running {len(DIMENSION_STEPS)} dimension-only steps, then STOPPING (simulated crash)...")
for step_name, layer, sql_path, target_table in DIMENSION_STEPS:
    rp.run_step(step_name, layer, sql_path, target_table, batch_id, run_id)

for step_name, sql_path, targets in rp.UPSERT_STEPS:
    rp.run_upsert_step(step_name, sql_path, targets, batch_id, run_id)

print("[PARTIAL RUN] Simulated crash here — facts were NEVER loaded this batch.")
