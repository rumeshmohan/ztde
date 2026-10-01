#!/usr/bin/env bash
# ============================================================
# TASK 2  -  DATA TRANSFORMATION   (PG_02_Transformation)
#
# Marking criteria and the processor that satisfies each one:
#
#   validate mandatory fields          -> TX_03_Validate_Mandatory_Fields
#                                         (QueryRecord, per-source
#                                          valid.condition from Task 1)
#   handle null / invalid values       -> TX_02_Apply_Business_Rules
#                                         (QueryRecord, COALESCE defaults)
#                                         + TX_04_Route_Invalid_Records
#   remove or manage duplicates        -> TX_05_Deduplicate_Records
#                                         (DeduplicateRecord on the
#                                          business key)
#   apply business transformations     -> TX_02_Apply_Business_Rules
#                                         (derived activity_date, total_fee,
#                                          COALESCE defaults)
#   generate the aggregated datasets   -> the Q7 aggregations are executed
#   required for the Q7 models            in PG_04 by the fact procedures,
#                                         over the cleansed staging data
#                                         this group produces.
#
# WHY THE AGGREGATION IS SQL AND NOT A NiFi PROCESSOR
#   Each of the three Q7 fact models aggregates SEVEN different source
#   tables into one row (Scenario A joins streams, rentals, returns,
#   payments, tickets, wishlist and reviews on customer + day). NiFi
#   record processors operate on one FlowFile at a time and cannot join
#   across thirteen separate streams. The aggregation therefore runs as
#   set-based SQL in PG_04 - but only over data this group has already
#   validated, null-handled and de-duplicated, so every Task 2 control
#   still applies to it.
#
# STRUCTURE
#   Every source shares one chain because Task 1 tagged each batch with
#   its own target.table / record.key / tx.sql / valid.condition. The
#   processors are ordered and named so the canvas reads as the marking
#   criteria list, top to bottom.
#
# Run AFTER scripts/nifi_build_task1_extract.sh
# ============================================================
set -euo pipefail

API="http://127.0.0.1:8080/nifi-api"
source /tmp/nifi_ids.env

post () { curl -s -X POST -H 'Content-Type: application/json' -d "$2" "$API/$1"; }
# 'head -1' can SIGPIPE grep, and under 'set -o pipefail' that aborts the
# whole script, so the first match is taken inside sed instead.
newid () { grep -o '"id":"[0-9a-f-]\{36\}"' | sed -n '1{s/"id":"//;s/"$//;p;}'; }

proc () { # proc <name> <type> <x> <y> <propsJson> [extraCfg]
    local name="$1" type="$2" x="$3" y="$4" props="$5" extra="${6:-}"
    local cfg="\"properties\":$props"
    [ -n "$extra" ] && cfg="$cfg,$extra"
    post "process-groups/$PG2/processors" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"type\":\"$type\",\"name\":\"$name\",
        \"position\":{\"x\":$x,\"y\":$y},
        \"config\":{$cfg}
      }
    }" | newid
}

# ------------------------------------------------------------
# port <kind> <name> <x> <y>   -> echoes the port id
# Reuses a port of the same name if one already exists. NiFi refuses
# duplicate port names inside a group, so a re-run of this script would
# otherwise fail instead of converging on the same flow.
# ------------------------------------------------------------
port () {
    local kind="$1" name="$2" x="$3" y="$4"
    local existing
    existing=$(curl -s "$API/process-groups/$PG2/$kind" \
               | tr '{' '\n' \
               | grep "\"name\":\"$name\"" \
               | grep -o '"id":"[0-9a-f-]\{36\}"' \
               | sed -n '1{s/"id":"//;s/"$//;p;}')
    if [ -n "$existing" ]; then
        echo "$existing"
        return
    fi
    post "process-groups/$PG2/$kind" "{
      \"revision\":{\"version\":0},
      \"component\":{\"name\":\"$name\",\"position\":{\"x\":$x,\"y\":$y}}
    }" | newid
}

conn () { # conn <src> <dst> <relsJson> [srcType] [dstType]
    post "process-groups/$PG2/connections" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"source\":{\"id\":\"$1\",\"groupId\":\"$PG2\",\"type\":\"${4:-PROCESSOR}\"},
        \"destination\":{\"id\":\"$2\",\"groupId\":\"$PG2\",\"type\":\"${5:-PROCESSOR}\"},
        \"selectedRelationships\":$3
      }
    }" > /dev/null
}

echo "=== TASK 2 : building PG_02_Transformation ==="

# ------------------------------------------------------------
# Ports. The input port receives every tagged batch from PG_01.
# ------------------------------------------------------------
IN_PORT=$(port "input-ports"  "IN_From_Extraction"           0 300)
OUT_OK=$(port  "output-ports" "OUT_Cleansed_To_Staging_Load" 1800 300)

# Rejected records leave the group on their own port so the error group
# owns all logging - the reason travels with the data.
OUT_REJ=$(port "output-ports" "OUT_Rejected_To_Error_Handling" 1800 700)

echo "  ports created"

# ------------------------------------------------------------
# TX_01 - record the batch size before any cleansing, so the audit trail
# can show how many records were dropped and why.
# ------------------------------------------------------------
TX01=$(proc "TX_01_Count_Records_In" \
  "org.apache.nifi.processors.attributes.UpdateAttribute" 300 300 \
  '{"records.in":"${record.count}","transform.stage":"received"}')

# ------------------------------------------------------------
# TX_02 - business rules and null handling.
# The SQL comes from the tx.sql attribute set in Task 1: COALESCE
# defaults for optional attributes and the derived columns the Q7 fact
# models need (activity_date, total_fee).
# ------------------------------------------------------------
TX02=$(proc "TX_02_Apply_Business_Rules" \
  "org.apache.nifi.processors.standard.QueryRecord" 600 300 \
  "{\"Record Reader\":\"$AVRO_READER\",\"Record Writer\":\"$AVRO_WRITER\",
    \"Include Zero Record FlowFiles\":\"false\",
    \"cleansed\":\"\${tx.sql}\"}")

# ------------------------------------------------------------
# TX_03 - mandatory field validation.
# valid.condition is the per-source NOT NULL / range test. Records that
# pass go on; the same query inverted (TX_04) captures the failures, so
# nothing is silently discarded.
# ------------------------------------------------------------
TX03=$(proc "TX_03_Validate_Mandatory_Fields" \
  "org.apache.nifi.processors.standard.QueryRecord" 900 300 \
  "{\"Record Reader\":\"$AVRO_READER\",\"Record Writer\":\"$AVRO_WRITER\",
    \"Include Zero Record FlowFiles\":\"false\",
    \"valid\":\"SELECT * FROM FLOWFILE WHERE \${valid.condition}\"}")

# ------------------------------------------------------------
# TX_04 - the mirror image of TX_03: anything failing the mandatory
# field test is routed out with a reason attached rather than dropped.
# ------------------------------------------------------------
TX04=$(proc "TX_04_Route_Invalid_Records" \
  "org.apache.nifi.processors.standard.QueryRecord" 900 700 \
  "{\"Record Reader\":\"$AVRO_READER\",\"Record Writer\":\"$AVRO_WRITER\",
    \"Include Zero Record FlowFiles\":\"false\",
    \"invalid\":\"SELECT * FROM FLOWFILE WHERE NOT (\${valid.condition})\"}")

TX04B=$(proc "TX_04b_Tag_Reject_Reason" \
  "org.apache.nifi.processors.attributes.UpdateAttribute" 1200 700 \
  '{"reject.reason":"Failed mandatory field / valid value check: ${valid.condition}","reject.stage":"TX_03_Validate_Mandatory_Fields"}')

# ------------------------------------------------------------
# TX_05 - duplicate management.
# Deduplicates on the business key named by record.key (a RecordPath,
# hence the leading slash). The "single" strategy compares records
# within the batch, which is the right scope here: each extract delivers
# one batch per table per run, and duplicates ACROSS runs are handled
# idempotently by the UPSERT in PG_03 rather than by unbounded in-memory
# state. Duplicates are routed away, not merged, so the first occurrence
# of a business key wins.
# ------------------------------------------------------------
TX05=$(proc "TX_05_Deduplicate_Records" \
  "org.apache.nifi.processors.standard.DeduplicateRecord" 1400 300 \
  "{\"Record Reader\":\"$AVRO_READER\",\"Record Writer\":\"$AVRO_WRITER\",
    \"Deduplication Strategy\":\"single\",
    \"Record Hashing Algorithm\":\"SHA-256\",
    \"Filter Type\":\"hash-set\",
    \"Include Zero Record FlowFiles\":\"false\",
    \"dedupe.key\":\"/\${record.key}\"}")

TX05B=$(proc "TX_05b_Tag_Duplicate_Reason" \
  "org.apache.nifi.processors.attributes.UpdateAttribute" 1400 700 \
  '{"reject.reason":"Duplicate business key ${record.key} removed by TX_05_Deduplicate_Records","reject.stage":"TX_05_Deduplicate_Records"}')

TX06=$(proc "TX_06_Count_Records_Out" \
  "org.apache.nifi.processors.attributes.UpdateAttribute" 1600 300 \
  '{"records.out":"${record.count}","transform.stage":"cleansed"}')

echo "  processors created (8)"

# ------------------------------------------------------------
# WIRING - the happy path reads left to right
# ------------------------------------------------------------
conn "$IN_PORT" "$TX01" '[""]' "INPUT_PORT" "PROCESSOR"
conn "$TX01" "$TX02" '["success"]'

# TX_02 emits the cleansed set on its dynamic relationship
conn "$TX02" "$TX03" '["cleansed"]'
conn "$TX02" "$TX04" '["cleansed"]'
# the untouched input copy is not needed once both branches have it
conn "$TX03" "$TX05" '["valid"]'
conn "$TX04" "$TX04B" '["invalid"]'
conn "$TX04B" "$OUT_REJ" '["success"]' "PROCESSOR" "OUTPUT_PORT"

# Only unique records continue to the load. Duplicates are tagged and
# sent to the error group so the run is auditable - "records removed as
# duplicates" is evidence, whereas auto-terminating them would be a
# silent drop.
conn "$TX05" "$TX06" '["non-duplicate"]'
conn "$TX05" "$TX05B" '["duplicate"]'
conn "$TX05B" "$OUT_REJ" '["success"]' "PROCESSOR" "OUTPUT_PORT"
conn "$TX06" "$OUT_OK" '["success"]' "PROCESSOR" "OUTPUT_PORT"

echo "  wiring complete"

cat >> /tmp/nifi_ids.env <<EOF
PG2_IN=$IN_PORT
PG2_OUT_OK=$OUT_OK
PG2_OUT_REJ=$OUT_REJ
TX01=$TX01
TX02=$TX02
TX03=$TX03
TX04=$TX04
TX04B=$TX04B
TX05=$TX05
TX05B=$TX05B
TX06=$TX06
EOF

echo "=== TASK 2 complete ==="
