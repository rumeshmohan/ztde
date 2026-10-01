#!/usr/bin/env bash
# ============================================================
# NiFi FLOW BUILDER  -  ABC Hub ETL Pipeline (Question 8)
#
# Builds the complete pipeline on the NiFi canvas through the NiFi
# REST API, so the flow is reproducible from source control instead of
# being hand-assembled by mouse.
#
# Canvas layout produced:
#
#   ABC_Hub_ETL_Pipeline
#     +-- PG_01_Extraction        Task 1  extract from operational DB
#     +-- PG_02_Transformation    Task 2  validate / clean / dedupe
#     +-- PG_03_Staging_Load      Task 2  land cleansed data
#     +-- PG_04_Warehouse_Load    Task 3  dimensions, then facts
#     +-- PG_05_Error_Handling    Task 4  failure routing and logging
#
# Prerequisites:
#   - NiFi running at http://127.0.0.1:8080/nifi
#   - PostgreSQL running, both databases created, procedures installed
#   - JDBC driver at C:\nifi\jdbc\postgresql-42.7.4.jar
#
# Usage:  bash scripts/nifi_build_flow.sh
# ============================================================
set -euo pipefail

API="http://127.0.0.1:8080/nifi-api"
JDBC_JAR="C:/nifi/jdbc/postgresql-42.7.4.jar"
DB_USER="postgres"
REJECT_DIR="C:/nifi/etl_logs/rejected"
ERROR_DIR="C:/nifi/etl_logs/errors"

# ------------------------------------------------------------
# helpers
# ------------------------------------------------------------
jqr () { # read a field from json on stdin
    # 'head -1' can SIGPIPE grep, and under 'set -o pipefail' that aborts
    # the script, so the first match is taken inside sed instead.
    grep -o "\"$1\":\"[^\"]*\"" | sed -n "1{s/\"$1\":\"//;s/\"\$//;p;}"
}

post () { curl -s -X POST -H 'Content-Type: application/json' -d "$2" "$API/$1"; }
put  () { curl -s -X PUT  -H 'Content-Type: application/json' -d "$2" "$API/$1"; }

ROOT=$(curl -s "$API/flow/process-groups/root" | jqr id)
echo "Root process group: $ROOT"

# ------------------------------------------------------------
# create_pg <parent> <name> <x> <y>   -> echoes new group id
# ------------------------------------------------------------
create_pg () {
    post "process-groups/$1/process-groups" "{
      \"revision\":{\"version\":0},
      \"component\":{\"name\":\"$2\",\"position\":{\"x\":$3,\"y\":$4}}
    }" | jqr id
}

# ------------------------------------------------------------
# create_proc <pg> <type> <name> <x> <y> <propsJson> [schedJson]
# ------------------------------------------------------------
create_proc () {
    local pg="$1" type="$2" name="$3" x="$4" y="$5" props="$6" sched="${7:-}"
    local cfg="\"properties\":$props"
    [ -n "$sched" ] && cfg="$cfg,$sched"
    post "process-groups/$pg/processors" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"type\":\"$type\",
        \"name\":\"$name\",
        \"position\":{\"x\":$x,\"y\":$y},
        \"config\":{$cfg}
      }
    }" | jqr id
}

# ------------------------------------------------------------
# connect <pg> <srcId> <srcType> <dstId> <dstType> <relsJson>
# ------------------------------------------------------------
connect () {
    post "process-groups/$1/connections" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"source\":{\"id\":\"$2\",\"groupId\":\"$1\",\"type\":\"$3\"},
        \"destination\":{\"id\":\"$4\",\"groupId\":\"$1\",\"type\":\"$5\"},
        \"selectedRelationships\":$6
      }
    }" > /dev/null
}

# ------------------------------------------------------------
# create_cs <pg> <type> <name> <propsJson>  -> echoes id
# ------------------------------------------------------------
create_cs () {
    post "process-groups/$1/controller-services" "{
      \"revision\":{\"version\":0},
      \"component\":{\"type\":\"$2\",\"name\":\"$3\",\"properties\":$4}
    }" | jqr id
}

# Enable a controller service, then confirm it really reached ENABLED.
# The API accepts the request before validation completes, so a silent
# "success" here would hide an invalid configuration.
enable_cs () {
    local id="$1" name="$2"
    local ver=$(curl -s "$API/controller-services/$id" | grep -o '"version":[0-9]*' | head -1 | grep -o '[0-9]*')
    put "controller-services/$id/run-status" \
        "{\"revision\":{\"version\":$ver},\"state\":\"ENABLED\"}" > /dev/null

    local state=""
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        state=$(curl -s "$API/controller-services/$id" | grep -o '"state":"[A-Z_]*"' | head -1 | sed 's/.*://;s/"//g')
        [ "$state" = "ENABLED" ] && break
    done

    if [ "$state" != "ENABLED" ]; then
        echo "  ERROR: controller service '$name' is $state, not ENABLED" >&2
        curl -s "$API/controller-services/$id" \
          | grep -o '"validationErrors":\[[^]]*\]' | head -1 >&2
        exit 1
    fi
    echo "  ENABLED  $name"
}


# ------------------------------------------------------------
# drop_queues <processGroupId>
# Recursively empties every connection queue in a group and its children.
# ------------------------------------------------------------
drop_queues () {
    local pg="$1"

    for c in $(curl -s "$API/process-groups/$pg/connections" \
               | tr '{' '\n' \
               | grep -o '"id":"[0-9a-f-]\{36\}"' \
               | sed 's/"id":"//;s/"//' | sort -u); do
        curl -s -X POST "$API/flowfile-queues/$c/drop-requests" > /dev/null || true
    done

    for child in $(curl -s "$API/process-groups/$pg/process-groups" \
                   | tr '{' '\n' \
                   | grep -o '"id":"[0-9a-f-]\{36\}"' \
                   | sed 's/"id":"//;s/"//' | sort -u); do
        [ "$child" = "$pg" ] && continue
        drop_queues "$child"
    done
}


# ============================================================
# IDEMPOTENT REBUILD
# Remove any previous build of this pipeline so the script can be run
# repeatedly without stacking duplicate copies on the canvas.
# ============================================================
# EVERY previous copy is removed, not just the first one found. A build
# that failed part way through still leaves a named group behind, and a
# leftover copy makes the next run fail with "a port with the same name
# already exists" rather than rebuilding cleanly.
OLD_IDS=$(curl -s "$API/flow/process-groups/$ROOT" \
      | tr '{' '\n' \
      | grep -o '"id":"[0-9a-f-]\{36\}","name":"ABC_Hub_ETL_Pipeline"' \
      | sed 's/"id":"//;s/","name.*//' | sort -u)

for OLD in $OLD_IDS; do
    echo "Removing previous build ($OLD)..."
    put "flow/process-groups/$OLD" \
        "{\"id\":\"$OLD\",\"state\":\"STOPPED\",\"disconnectedNodeAcknowledged\":false}" > /dev/null || true
    sleep 2
    # controller services must be disabled before the group can be deleted
    put "flow/process-groups/$OLD/controller-services" \
        "{\"id\":\"$OLD\",\"state\":\"DISABLED\",\"disconnectedNodeAcknowledged\":false}" > /dev/null || true
    sleep 2
    # A group holding queued FlowFiles cannot be deleted, so every queue
    # inside it is emptied first. Without this a previous run that hit
    # back pressure would be undeletable and block the rebuild.
    drop_queues "$OLD"

    OVER=$(curl -s "$API/process-groups/$OLD" | grep -o '"version":[0-9]*' | sed -n '1s/[^0-9]*//p')
    DEL=$(curl -s -X DELETE "$API/process-groups/$OLD?version=$OVER")
    case "$DEL" in
        *'"id"'*) : ;;                       # deleted
        *) echo "  WARNING: could not delete $OLD - $DEL" >&2 ;;
    esac
    sleep 1
done


# ============================================================
# TOP-LEVEL PROCESS GROUP
# ============================================================
echo "Creating process groups..."
MAIN=$(create_pg "$ROOT" "ABC_Hub_ETL_Pipeline" 400 40)

PG1=$(create_pg "$MAIN" "PG_01_Extraction"      0    0)
PG2=$(create_pg "$MAIN" "PG_02_Transformation"  480  0)
PG3=$(create_pg "$MAIN" "PG_03_Staging_Load"    960  0)
PG4=$(create_pg "$MAIN" "PG_04_Warehouse_Load"  1440 0)
PG5=$(create_pg "$MAIN" "PG_05_Error_Handling"  720  360)

echo "  MAIN=$MAIN  PG1=$PG1  PG2=$PG2  PG3=$PG3  PG4=$PG4  PG5=$PG5"


# ============================================================
# CONTROLLER SERVICES  (defined on the parent group so every child
# process group shares one connection pool per database)
# ============================================================
echo "Creating controller services..."

DBCP_OPER=$(create_cs "$MAIN" \
  "org.apache.nifi.dbcp.DBCPConnectionPool" "OperationalDB_Pool" "{
    \"Database Connection URL\":\"jdbc:postgresql://localhost:5432/abc_hub_operational\",
    \"Database Driver Class Name\":\"org.postgresql.Driver\",
    \"Database Driver Locations\":\"$JDBC_JAR\",
    \"Database User\":\"$DB_USER\",
    \"Max Total Connections\":\"8\"
  }")

DBCP_ANAL=$(create_cs "$MAIN" \
  "org.apache.nifi.dbcp.DBCPConnectionPool" "AnalyticalDB_Pool" "{
    \"Database Connection URL\":\"jdbc:postgresql://localhost:5432/abc_hub_analytical\",
    \"Database Driver Class Name\":\"org.postgresql.Driver\",
    \"Database Driver Locations\":\"$JDBC_JAR\",
    \"Database User\":\"$DB_USER\",
    \"Max Total Connections\":\"8\"
  }")

# Record readers / writers - the pipeline moves data as Avro records so
# the record-aware processors can validate and transform field by field.
# Avro carries its schema inside the payload, so no external schema
# registry is needed and the flow adapts if a source column is added.
# NOTE: property NAMES are display names, but allowable VALUES are the
# internal identifiers ("embedded-avro-schema", not "Use Embedded Avro Schema").
AVRO_READER=$(create_cs "$MAIN" \
  "org.apache.nifi.avro.AvroReader" "AvroReader" \
  '{"Schema Access Strategy":"embedded-avro-schema"}')

# "inherit-record-schema" makes the writer take its schema from whatever
# the upstream processor produced, so one writer serves every table in
# the pipeline instead of needing 13 hand-written Avro schemas.
AVRO_WRITER=$(create_cs "$MAIN" \
  "org.apache.nifi.avro.AvroRecordSetWriter" "AvroWriter" \
  '{"Schema Write Strategy":"avro-embedded","Schema Access Strategy":"inherit-record-schema"}')

# Used only for writing human-readable rejected records to disk.
JSON_WRITER=$(create_cs "$MAIN" \
  "org.apache.nifi.json.JsonRecordSetWriter" "JsonWriter" \
  '{"Pretty Print JSON":"true"}')

enable_cs "$DBCP_OPER"   "OperationalDB_Pool"
enable_cs "$DBCP_ANAL"   "AnalyticalDB_Pool"
enable_cs "$AVRO_READER" "AvroReader"
enable_cs "$AVRO_WRITER" "AvroWriter"
enable_cs "$JSON_WRITER" "JsonWriter"

# Persist the ids so the follow-up scripts can reference them
cat > /tmp/nifi_ids.env <<EOF
ROOT=$ROOT
MAIN=$MAIN
PG1=$PG1
PG2=$PG2
PG3=$PG3
PG4=$PG4
PG5=$PG5
DBCP_OPER=$DBCP_OPER
DBCP_ANAL=$DBCP_ANAL
AVRO_READER=$AVRO_READER
AVRO_WRITER=$AVRO_WRITER
JSON_WRITER=$JSON_WRITER
EOF
echo "Stage 1 complete - ids written to /tmp/nifi_ids.env"
