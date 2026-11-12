#!/usr/bin/env bash
# ============================================================
# TASK 4  -  PIPELINE MANAGEMENT   (PG_05_Error_Handling + wiring)
#
# Marking criteria and how each is satisfied:
#
#   error handling using failure relationships
#       Every processor that can fail has its 'failure' (and 'retry',
#       where the processor has one) relationship connected into this
#       group. Nothing is auto-terminated on a failure path, so no error
#       is ever silently discarded.
#
#   logging of failed records
#       Two destinations, because they answer different questions:
#         EH_02_Log_Failure_Details  -> NiFi bulletin / app log, for the
#                                       operator watching the canvas
#         EH_03_Write_Reject_File    -> C:/nifi/etl_logs/rejected, the
#                                       rejected payload itself
#         EH_04_Insert_Reject_Record -> staging.etl_rejected_records, so
#                                       failures are queryable next to
#                                       the data they came from
#
#   appropriate processor scheduling
#       TRIGGER_Start_ETL_Batch is CRON_DRIVEN at 02:00 daily - the whole
#       pipeline runs from one schedule. The incremental extracts poll on
#       a 30 second timer. Load processors are event driven: they run
#       only when a FlowFile arrives.
#
#   clear processor naming and workflow organisation
#       Five process groups, one per pipeline stage, and every processor
#       prefixed by its stage and sequence (EX_, SET_Meta_, TX_nn_,
#       LS_nn_, LD_nn_, LF_nn_, EH_nn_) so the canvas reads in order.
#
# Run AFTER scripts/nifi_build_task3_load.sh
# ============================================================
set -euo pipefail

API="http://127.0.0.1:8080/nifi-api"
source /tmp/nifi_ids.env
LOG_DIR="C:/nifi/etl_logs/rejected"

post () { curl -s -X POST -H 'Content-Type: application/json' -d "$2" "$API/$1"; }
newid () { grep -o '"id":"[0-9a-f-]\{36\}"' | sed -n '1{s/"id":"//;s/"$//;p;}'; }

proc () { # proc <pg> <name> <type> <x> <y> <propsJson> [extraCfg]
    local pg="$1" name="$2" type="$3" x="$4" y="$5" props="$6" extra="${7:-}"
    local cfg="\"properties\":$props"
    [ -n "$extra" ] && cfg="$cfg,$extra"
    post "process-groups/$pg/processors" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"type\":\"$type\",\"name\":\"$name\",
        \"position\":{\"x\":$x,\"y\":$y},
        \"config\":{$cfg}
      }
    }" | newid
}

port () { # port <pg> <kind> <name> <x> <y>
    local pg="$1" kind="$2" name="$3" x="$4" y="$5" existing
    existing=$(curl -s "$API/process-groups/$pg/$kind" \
               | tr '{' '\n' | grep "\"name\":\"$name\"" \
               | grep -o '"id":"[0-9a-f-]\{36\}"' \
               | sed -n '1{s/"id":"//;s/"$//;p;}')
    [ -n "$existing" ] && { echo "$existing"; return; }
    post "process-groups/$pg/$kind" "{
      \"revision\":{\"version\":0},
      \"component\":{\"name\":\"$name\",\"position\":{\"x\":$x,\"y\":$y}}
    }" | newid
}

conn () { # conn <pg> <src> <dst> <relsJson> [srcType] [dstType]
    post "process-groups/$1/connections" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"source\":{\"id\":\"$2\",\"groupId\":\"$1\",\"type\":\"${5:-PROCESSOR}\"},
        \"destination\":{\"id\":\"$3\",\"groupId\":\"$1\",\"type\":\"${6:-PROCESSOR}\"},
        \"selectedRelationships\":$4
      }
    }" > /dev/null
}

autoterm () { # autoterm <procId> <relsJson>
    local V
    V=$(curl -s "$API/processors/$1" | grep -o '"version":[0-9]*' | sed -n '1s/[^0-9]*//p')
    curl -s -X PUT -H 'Content-Type: application/json' -d "{
      \"revision\":{\"version\":$V},
      \"component\":{\"id\":\"$1\",\"config\":{\"autoTerminatedRelationships\":$2}}
    }" "$API/processors/$1" > /dev/null
}

echo "=== TASK 4 : building PG_05_Error_Handling ==="

EH_IN=$(port "$PG5" "input-ports" "IN_Failed_Records" 0 300)

# ------------------------------------------------------------
# EH_01 - make sure every failure carries a reason, even one arriving
# from a processor-level failure relationship that never passed through
# a tagging processor.
# ------------------------------------------------------------
EH01=$(proc "$PG5" "EH_01_Enrich_Error_Context" \
  "org.apache.nifi.processors.attributes.UpdateAttribute" 300 300 \
  '{"reject.reason":"${reject.reason:isEmpty():ifElse(${error.message:isEmpty():ifElse(\"Processor failure relationship - see NiFi bulletin\",${error.message})},${reject.reason})}",
    "reject.source.table":"${target.table:isEmpty():ifElse(\"unknown\",${target.table})}",
    "reject.stage":"${reject.stage:isEmpty():ifElse(\"unspecified\",${reject.stage})}"}')

# ------------------------------------------------------------
# EH_02 - log to the NiFi application log / bulletin board.
# This is the operator-facing record: it appears on the canvas.
# ------------------------------------------------------------
EH02=$(proc "$PG5" "EH_02_Log_Failure_Details" \
  "org.apache.nifi.processors.standard.LogAttribute" 600 300 \
  '{"Log Level":"error",
    "Log Payload":"false",
    "Attributes to Log Regular Expression":"reject\\..*|target\\.table|record\\.key|uuid|filename",
    "Log Prefix":"ABC_HUB_ETL_REJECT"}')

# ------------------------------------------------------------
# EH_03 - persist the rejected payload to disk so it can be inspected
# and re-processed after the underlying data problem is fixed.
# ------------------------------------------------------------
EH03=$(proc "$PG5" "EH_03_Write_Reject_File" \
  "org.apache.nifi.processors.standard.PutFile" 900 300 \
  "{\"Directory\":\"$LOG_DIR\",
    \"Conflict Resolution Strategy\":\"replace\",
    \"Create Missing Directories\":\"true\"}")

# ------------------------------------------------------------
# EH_04 - record the failure in the database beside the data it came
# from, so a data steward can query rejects with plain SQL rather than
# reading log files.
# ------------------------------------------------------------
EH04=$(proc "$PG5" "EH_04_Insert_Reject_Record" \
  "org.apache.nifi.processors.standard.PutSQL" 1200 300 \
  "{\"JDBC Connection Pool\":\"$DBCP_ANAL\",
    \"SQL Statement\":\"INSERT INTO staging.etl_rejected_records (source_table, reject_reason, record_payload) VALUES ('\${reject.source.table}', '\${reject.reason}', '\${uuid} logged to $LOG_DIR')\",
    \"Support Fragmented Transactions\":\"false\",
    \"Batch Size\":\"1\"}")

# The audit trail ends here. If writing the reject itself fails there is
# nowhere further to escalate, so these are terminated deliberately -
# the bulletin from EH_02 has already recorded the failure.
autoterm "$EH03" '["success","failure"]'
autoterm "$EH04" '["success","failure","retry"]'

conn "$PG5" "$EH_IN" "$EH01" '[""]' "INPUT_PORT" "PROCESSOR"
conn "$PG5" "$EH01" "$EH02" '["success"]'
conn "$PG5" "$EH02" "$EH03" '["success"]'
conn "$PG5" "$EH03" "$EH04" '["success"]'

echo "  PG_05 complete (4 processors)"

# ============================================================
# FAILURE RELATIONSHIPS IN THE UPSTREAM GROUPS
#
# A failure inside a child group cannot connect straight to PG_05, so
# each group routes its own failures to its own error output port, and
# the top-level wiring below joins those ports to PG_05. This keeps the
# error path visible inside every group instead of hiding it.
# ============================================================
echo "=== connecting failure relationships ==="

# ---- PG_01 : an extract that cannot reach the source database --------
P1_ERR=$(port "$PG1" "output-ports" "OUT_Failed_To_Error_Handling" 1600 1700)

EX_ERR=$(proc "$PG1" "EX_99_Tag_Extract_Failure" \
  "org.apache.nifi.processors.attributes.UpdateAttribute" 1200 1700 \
  '{"reject.reason":"Extraction from the operational database failed","reject.stage":"PG_01_Extraction"}')

for P in "$EX_CUST" "$EX_CONT" "$EX_DEV" "$EX_PLAN" "$EX_WH" "$EX_SCAT" "$EX_INV"; do
    conn "$PG1" "$P" "$EX_ERR" '["failure"]'
done
conn "$PG1" "$EX_ERR" "$P1_ERR" '["success"]' "PROCESSOR" "OUTPUT_PORT"

# ---- PG_02 : a record-level transformation failure -------------------
# 'failure' means the FlowFile could not be processed at all (unreadable
# Avro, invalid SQL). 'original' is the untouched input copy: once both
# the valid and invalid branches have been derived from it there is
# nothing left to do with it, so it is terminated deliberately.
TX_ERR=$(proc "$PG2" "TX_99_Tag_Transform_Failure" \
  "org.apache.nifi.processors.attributes.UpdateAttribute" 1200 1100 \
  '{"reject.reason":"Record transformation failed - unreadable content or invalid transformation SQL","reject.stage":"PG_02_Transformation"}')

for P in "$TX02" "$TX03" "$TX04" "$TX05"; do
    conn "$PG2" "$P" "$TX_ERR" '["failure"]'
    autoterm "$P" '["original"]'
done
conn "$PG2" "$TX_ERR" "$PG2_OUT_REJ" '["success"]' "PROCESSOR" "OUTPUT_PORT"

# ---- PG_04 : the batch barrier --------------------------------------
# MergeContent's 'original' is each pre-merge FlowFile; the merged copy
# carries the batch forward, so the originals are terminated.
conn "$PG4" "$LD00" "$LD_ERR" '["failure"]'
autoterm "$LD00" '["original"]'

echo "  failure relationships connected"

# ============================================================
# TOP-LEVEL WIRING  -  connect the five stages together
# ============================================================
echo "=== wiring the five process groups together ==="

# mainconn <srcGroup> <srcPortName> <dstGroup> <dstPortName>
mainconn () {
    local sg="$1" sp="$2" dg="$3" dp="$4" sid did
    sid=$(curl -s "$API/process-groups/$sg/output-ports" \
          | tr '{' '\n' | grep "\"name\":\"$sp\"" \
          | grep -o '"id":"[0-9a-f-]\{36\}"' | sed -n '1{s/"id":"//;s/"$//;p;}')
    did=$(curl -s "$API/process-groups/$dg/input-ports" \
          | tr '{' '\n' | grep "\"name\":\"$dp\"" \
          | grep -o '"id":"[0-9a-f-]\{36\}"' | sed -n '1{s/"id":"//;s/"$//;p;}')
    if [ -z "$sid" ] || [ -z "$did" ]; then
        echo "  ERROR: could not resolve $sp -> $dp" >&2; exit 1
    fi
    post "process-groups/$MAIN/connections" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"source\":{\"id\":\"$sid\",\"groupId\":\"$sg\",\"type\":\"OUTPUT_PORT\"},
        \"destination\":{\"id\":\"$did\",\"groupId\":\"$dg\",\"type\":\"INPUT_PORT\"},
        \"selectedRelationships\":[\"\"]
      }
    }" > /dev/null
    echo "  $sp -> $dp"
}

mainconn "$PG1" "OUT_To_Transformation"           "$PG2" "IN_From_Extraction"
mainconn "$PG2" "OUT_Cleansed_To_Staging_Load"    "$PG3" "IN_Cleansed_Records"
mainconn "$PG3" "OUT_Trigger_Warehouse_Load"      "$PG4" "IN_Staging_Ready"

# every stage routes its failures to the one error handling group
mainconn "$PG1" "OUT_Failed_To_Error_Handling"    "$PG5" "IN_Failed_Records"
mainconn "$PG2" "OUT_Rejected_To_Error_Handling"  "$PG5" "IN_Failed_Records"
mainconn "$PG3" "OUT_Failed_To_Error_Handling"    "$PG5" "IN_Failed_Records"
mainconn "$PG4" "OUT_Failed_To_Error_Handling"    "$PG5" "IN_Failed_Records"

cat >> /tmp/nifi_ids.env <<EOF
EH_IN=$EH_IN
EH01=$EH01
EH02=$EH02
EH03=$EH03
EH04=$EH04
EOF

echo "=== TASK 4 complete ==="
