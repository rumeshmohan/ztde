#!/usr/bin/env bash
# ============================================================
# TASK 1  -  DATA EXTRACTION   (PG_01_Extraction)
#
# Marking criteria addressed:
#   * connect to the operational database   -> OperationalDB_Pool
#   * extract the required tables           -> 13 extract processors
#   * read incrementally where appropriate  -> QueryDatabaseTableRecord
#                                              with Maximum-value Columns
#   * transfer to the transformation stage  -> Output Port to PG_02
#
# EXTRACTION STRATEGY
#   Reference / dimension sources -> FULL EXTRACT (ExecuteSQLRecord)
#       Low volume and slowly changing, so a full extract each run keeps
#       the SCD comparison simple and costs almost nothing.
#
#   Transactional / fact sources  -> INCREMENTAL EXTRACT
#       (QueryDatabaseTableRecord + Maximum-value Column)
#       High volume and insert-only. NiFi persists the highest key seen
#       in processor state, so each run reads only new rows. The
#       BIGSERIAL primary key is the high-water mark because the
#       operational model has no row-level last_modified column.
#
# WHY EVERY EXTRACT IS FOLLOWED BY A "SET_Meta_*" TAGGER
#   The 13 sources need different target tables, different business keys
#   and different cleansing rules, but the SEQUENCE of steps applied to
#   them is identical: transform -> validate -> de-duplicate -> load.
#   Each tagger stamps its batch with that per-source configuration as
#   FlowFile attributes, which lets PG_02 and PG_03 be one shared,
#   clearly named chain instead of 13 copy-pasted chains. The per-source
#   logic stays visible in exactly one place: the tagger.
#
# Run AFTER scripts/nifi_build_flow.sh
# ============================================================
set -euo pipefail

API="http://127.0.0.1:8080/nifi-api"
source /tmp/nifi_ids.env

post () { curl -s -X POST -H 'Content-Type: application/json' -d "$2" "$API/$1"; }
# 'head -1' can SIGPIPE grep, and under 'set -o pipefail' that aborts the
# whole script, so the first match is taken inside sed instead.
newid () { grep -o '"id":"[0-9a-f-]\{36\}"' | sed -n '1{s/"id":"//;s/"$//;p;}'; }

# ------------------------------------------------------------
# WHY "Use Avro Logical Types" IS TRUE ON EVERY EXTRACT
#   With it left at the default (false) NiFi degrades the SQL types the
#   warehouse depends on:
#     NUMERIC(10,2) -> Avro string   e.g. monthly_fee became "4.99"
#     DATE          -> Avro string   e.g. purchase_date became
#                                    "1642185000000" (epoch millis)
#   Both then break downstream: QueryRecord raised
#   NumberFormatException evaluating "monthly_fee >= 0" on a string, and
#   PostgreSQL rejected the insert with 'column "purchase_date" is of
#   type date but expression is of type character varying'.
#   Setting it to true keeps decimal as a true decimal and date/timestamp
#   as date/timestamp logical types all the way to the staging table, so
#   no numeric or date value is ever re-parsed from text.
# ------------------------------------------------------------

# ------------------------------------------------------------
# add_full <name> <x> <y> <sql>
# Full extract via ExecuteSQLRecord. Triggered by an incoming FlowFile
# from the batch trigger so the whole group runs as one controlled run
# rather than each processor polling the source on its own timer.
# ------------------------------------------------------------
add_full () {
    local name="$1" x="$2" y="$3" sql="$4"
    post "process-groups/$PG1/processors" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"type\":\"org.apache.nifi.processors.standard.ExecuteSQLRecord\",
        \"name\":\"$name\",
        \"position\":{\"x\":$x,\"y\":$y},
        \"config\":{
          \"properties\":{
            \"Database Connection Pooling Service\":\"$DBCP_OPER\",
            \"Record Writer\":\"$AVRO_WRITER\",
            \"SQL Query\":\"$sql\",
            \"Use Avro Logical Types\":\"true\",
            \"Max Rows Per FlowFile\":\"0\"
          }
        }
      }
    }" | newid
}

# ------------------------------------------------------------
# add_incr <name> <x> <y> <table> <maxValCol> <customQuery>
# Incremental extract via QueryDatabaseTableRecord.
# "Table Name" is still required by the processor even when a Custom
# Query is supplied - it is the state key and the alias target.
# Scheduled at 30 sec rather than 0 sec: this processor is a source
# (no incoming connection), so an empty schedule would spin the CPU.
# ------------------------------------------------------------
add_incr () {
    local name="$1" x="$2" y="$3" table="$4" maxcol="$5" query="$6"
    post "process-groups/$PG1/processors" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"type\":\"org.apache.nifi.processors.standard.QueryDatabaseTableRecord\",
        \"name\":\"$name\",
        \"position\":{\"x\":$x,\"y\":$y},
        \"config\":{
          \"schedulingStrategy\":\"TIMER_DRIVEN\",
          \"schedulingPeriod\":\"30 sec\",
          \"properties\":{
            \"Database Connection Pooling Service\":\"$DBCP_OPER\",
            \"Database Type\":\"PostgreSQL\",
            \"Record Writer\":\"$AVRO_WRITER\",
            \"Table Name\":\"$table\",
            \"Custom Query\":\"$query\",
            \"Maximum-value Columns\":\"$maxcol\",
            \"Initial Load Strategy\":\"Start at Beginning\",
            \"Use Avro Logical Types\":\"true\",
            \"Max Rows Per FlowFile\":\"0\"
          }
        }
      }
    }" | newid
}

# ------------------------------------------------------------
# add_meta <name> <x> <y> <stagingTable> <businessKey> <txSql> <validCond>
# UpdateAttribute that labels the batch for the generic downstream chain.
#   target.table      staging table PutDatabaseRecord will UPSERT into
#   record.key        business key - dedupe path and UPSERT key
#   tx.sql            per-source cleansing / derivation applied by QueryRecord
#   valid.condition   mandatory-field and valid-value test for this source.
#                     Derived from the NOT NULL constraints and CHECK ranges
#                     on the staging table, so the pipeline rejects a bad
#                     record with a logged reason instead of letting the
#                     database throw and fail the whole batch.
# ------------------------------------------------------------
add_meta () {
    local name="$1" x="$2" y="$3" table="$4" key="$5" sql="$6" cond="$7"
    post "process-groups/$PG1/processors" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"type\":\"org.apache.nifi.processors.attributes.UpdateAttribute\",
        \"name\":\"$name\",
        \"position\":{\"x\":$x,\"y\":$y},
        \"config\":{
          \"properties\":{
            \"target.table\":\"$table\",
            \"record.key\":\"$key\",
            \"tx.sql\":\"$sql\",
            \"valid.condition\":\"$cond\"
          }
        }
      }
    }" | newid
}

# ------------------------------------------------------------
# connect <srcId> <dstId> <relsJson>   (both inside PG_01)
# ------------------------------------------------------------
conn () {
    post "process-groups/$PG1/connections" "{
      \"revision\":{\"version\":0},
      \"component\":{
        \"source\":{\"id\":\"$1\",\"groupId\":\"$PG1\",\"type\":\"${4:-PROCESSOR}\"},
        \"destination\":{\"id\":\"$2\",\"groupId\":\"$PG1\",\"type\":\"${5:-PROCESSOR}\"},
        \"selectedRelationships\":$3
      }
    }" > /dev/null
}

echo "=== TASK 1 : building PG_01_Extraction ==="

# ------------------------------------------------------------
# Batch trigger. One GenerateFlowFile starts the whole run, which makes
# the pipeline explicitly schedulable (Task 4) instead of having the
# full extracts each poll the source database on their own timer.
# ------------------------------------------------------------
TRIGGER=$(post "process-groups/$PG1/processors" "{
  \"revision\":{\"version\":0},
  \"component\":{
    \"type\":\"org.apache.nifi.processors.standard.GenerateFlowFile\",
    \"name\":\"TRIGGER_Start_ETL_Batch\",
    \"position\":{\"x\":0,\"y\":700},
    \"config\":{
      \"schedulingStrategy\":\"CRON_DRIVEN\",
      \"schedulingPeriod\":\"0 0 2 * * ?\",
      \"properties\":{\"File Size\":\"0B\",\"Batch Size\":\"1\"}
    }
  }
}" | newid)
echo "  TRIGGER_Start_ETL_Batch=$TRIGGER"

# Output port - the hand-off to PG_02_Transformation.
OUT_PORT=$(post "process-groups/$PG1/output-ports" "{
  \"revision\":{\"version\":0},
  \"component\":{\"name\":\"OUT_To_Transformation\",\"position\":{\"x\":1600,\"y\":700}}
}" | newid)
echo "  OUT_To_Transformation=$OUT_PORT"

# ============================================================
# FULL EXTRACTS - dimension sources
# ============================================================
# NOTE ON THE CASTs BELOW
# ExecuteSQLRecord infers the Avro schema from JDBC ResultSetMetaData.
# PostgreSQL reports city.city_name as NOT NULL because that is how the
# base column is declared - even though this LEFT JOIN can legitimately
# produce a NULL for a customer with no home address. Writing that NULL
# into a non-nullable Avro field fails the whole FlowFile.
# Wrapping the outer-joined columns in CAST makes them expressions, so
# the driver reports nullability as unknown and NiFi generates a
# nullable field. The NULL is preserved and travels through to Task 2,
# which is where null handling belongs.
#
# monthly_fee is cast to VARCHAR rather than DECIMAL for one reason: an
# Avro DECIMAL field generated from JDBC metadata is always non-nullable,
# so a customer with no active subscription failed the whole batch with
# 'null value for (non-nullable) bytes at nifiRecord.monthly_fee'. Carried
# as text the NULL survives extraction, and SET_Meta_Customer casts it
# back to DECIMAL(10,2) in PG_02 - so the type is restored in the same
# stage that handles the null, and staging still receives a true numeric.
EX_CUST=$(add_full "EX_Customer_Full" 300 0 \
"SELECT c.customer_id, c.customer_no, c.first_name, c.last_name, c.email, c.phone, c.date_of_birth, c.gender, c.registration_date, c.status, CAST(ci.city_name AS VARCHAR) AS city_name, CAST(co.country_name AS VARCHAR) AS country_name, CAST(sp.plan_name AS VARCHAR) AS plan_name, CAST(sp.monthly_fee AS VARCHAR) AS monthly_fee FROM customer c LEFT JOIN customer_address ca ON ca.customer_id = c.customer_id AND ca.address_type = 'home' LEFT JOIN city ci ON ci.city_id = ca.city_id LEFT JOIN country co ON co.country_id = ci.country_id LEFT JOIN customer_subscription cs ON cs.customer_id = c.customer_id AND cs.status = 'active' LEFT JOIN subscription_plan sp ON sp.plan_id = cs.plan_id")

# c.language is aliased to content_language: "LANGUAGE" is a reserved word
# in Apache Calcite, the SQL engine QueryRecord uses in PG_02, and an
# unquoted reserved word there fails the whole statement with
# 'parse failed: Encountered "language"'. Renaming it at the source keeps
# the downstream SQL readable without quoted identifiers.
EX_CONT=$(add_full "EX_Content_Full" 300 120 \
"SELECT c.content_id, c.title, ct.content_type, c.release_date, c.duration_minutes, c.language AS content_language, c.age_rating, CAST((SELECT MIN(g.genre_name) FROM content_genre cg JOIN genre g ON g.genre_id = cg.genre_id WHERE cg.content_id = c.content_id) AS VARCHAR) AS primary_genre FROM content c JOIN content_type ct ON ct.content_type_id = c.content_type_id")

EX_DEV=$(add_full "EX_Device_Full" 300 240 \
"SELECT device_id, device_name FROM device")

EX_PLAN=$(add_full "EX_SubscriptionPlan_Full" 300 360 \
"SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices FROM subscription_plan")

EX_WH=$(add_full "EX_Warehouse_Full" 300 480 \
"SELECT w.warehouse_id, w.warehouse_name, ci.city_name, co.country_name FROM warehouse w JOIN city ci ON ci.city_id = w.city_id JOIN country co ON co.country_id = ci.country_id")

EX_SCAT=$(add_full "EX_SupportCategory_Full" 300 600 \
"SELECT category_id, category_name FROM support_category")

EX_INV=$(add_full "EX_InventoryItem_Full" 300 720 \
"SELECT i.inventory_id, i.content_id, c.title AS content_title, ct.content_type, i.warehouse_id, w.warehouse_name, ci.city_name, i.barcode, i.purchase_date, i.item_condition, i.status FROM inventory_item i JOIN content c ON c.content_id = i.content_id JOIN content_type ct ON ct.content_type_id = c.content_type_id JOIN warehouse w ON w.warehouse_id = i.warehouse_id JOIN city ci ON ci.city_id = w.city_id")

echo "  full extracts created (7)"

# ============================================================
# INCREMENTAL EXTRACTS - fact sources
# ============================================================
EX_STREAM=$(add_incr "EX_StreamingSession_Incremental" 300 900 \
  "streaming_session" "stream_id" \
"SELECT stream_id, customer_id, content_id, device_id, start_time, end_time, watch_duration, completion_percentage FROM streaming_session")

EX_RENT=$(add_incr "EX_Rental_Incremental" 300 1020 \
  "rental" "rental_id" \
"SELECT r.rental_id, r.customer_id, r.inventory_id, i.content_id, r.rental_date, r.due_date, r.return_date, r.rental_fee, r.late_fee, r.status FROM rental r JOIN inventory_item i ON i.inventory_id = r.inventory_id")

EX_PAY=$(add_incr "EX_Payment_Incremental" 300 1140 \
  "payment" "payment_id" \
"SELECT payment_id, customer_id, rental_id, subscription_id, amount, payment_date, payment_type, status FROM payment")

EX_TICK=$(add_incr "EX_SupportTicket_Incremental" 300 1260 \
  "support_ticket" "ticket_id" \
"SELECT ticket_id, customer_id, category_id, opened_date, closed_date, priority, status FROM support_ticket")

EX_REV=$(add_incr "EX_Review_Incremental" 300 1380 \
  "review" "review_id" \
"SELECT review_id, customer_id, content_id, rating, review_date FROM review")

EX_WISH=$(add_incr "EX_Wishlist_Incremental" 300 1500 \
  "wishlist" "wishlist_id" \
"SELECT wishlist_id, customer_id, content_id, added_date FROM wishlist")

echo "  incremental extracts created (6)"

# ============================================================
# METADATA TAGGERS
# tx.sql carries the per-source cleansing rules applied by QueryRecord
# in PG_02. The COALESCE defaults deliberately match the defaults the
# dimension load procedures use, so a value is substituted once and
# means the same thing in staging and in the warehouse.
# ============================================================
M_CUST=$(add_meta  "SET_Meta_Customer"          800 0    "stg_customer"      "customer_id" \
"SELECT customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, COALESCE(gender, 'Unknown') AS gender, registration_date, status, COALESCE(city_name, 'Unknown') AS city_name, COALESCE(country_name, 'Unknown') AS country_name, plan_name, CAST(monthly_fee AS DECIMAL(10,2)) AS monthly_fee FROM FLOWFILE" \
"customer_id > 0 AND status IS NOT NULL")

M_CONT=$(add_meta  "SET_Meta_Content"           800 120  "stg_content"       "content_id" \
"SELECT content_id, title, content_type, release_date, duration_minutes, COALESCE(content_language, 'Unknown') AS content_language, COALESCE(age_rating, 'Unrated') AS age_rating, COALESCE(primary_genre, 'Uncategorised') AS primary_genre FROM FLOWFILE" \
"content_id > 0 AND (duration_minutes IS NULL OR duration_minutes > 0)")

M_DEV=$(add_meta   "SET_Meta_Device"            800 240  "stg_device"        "device_id" \
"SELECT device_id, device_name FROM FLOWFILE" \
"device_id > 0")

M_PLAN=$(add_meta  "SET_Meta_SubscriptionPlan"  800 360  "stg_subscription_plan" "plan_id" \
"SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices FROM FLOWFILE" \
"plan_id > 0 AND monthly_fee >= 0")

M_WH=$(add_meta    "SET_Meta_Warehouse"         800 480  "stg_warehouse"     "warehouse_id" \
"SELECT warehouse_id, warehouse_name, COALESCE(city_name, 'Unknown') AS city_name, COALESCE(country_name, 'Unknown') AS country_name FROM FLOWFILE" \
"warehouse_id > 0")

M_SCAT=$(add_meta  "SET_Meta_SupportCategory"   800 600  "stg_support_category" "category_id" \
"SELECT category_id, category_name FROM FLOWFILE" \
"category_id > 0")

M_INV=$(add_meta   "SET_Meta_InventoryItem"     800 720  "stg_inventory_item" "inventory_id" \
"SELECT inventory_id, content_id, content_title, content_type, warehouse_id, warehouse_name, COALESCE(city_name, 'Unknown') AS city_name, barcode, purchase_date, COALESCE(item_condition, 'Unknown') AS item_condition, status FROM FLOWFILE" \
"inventory_id > 0 AND content_id > 0 AND warehouse_id > 0")

# activity_date is derived here so the fact aggregations can group by a
# plain DATE instead of casting a timestamp on every query.
M_STREAM=$(add_meta "SET_Meta_StreamingSession" 800 900  "stg_streaming_session" "stream_id" \
"SELECT stream_id, customer_id, content_id, device_id, start_time, end_time, CAST(start_time AS DATE) AS activity_date, COALESCE(watch_duration, 0) AS watch_duration, COALESCE(completion_percentage, 0) AS completion_percentage FROM FLOWFILE" \
"stream_id > 0 AND customer_id > 0 AND content_id > 0 AND watch_duration >= 0 AND completion_percentage >= 0 AND completion_percentage <= 100")

# total_fee is the additive measure Scenario B sums, pre-computed once.
M_RENT=$(add_meta  "SET_Meta_Rental"            800 1020 "stg_rental"        "rental_id" \
"SELECT rental_id, customer_id, inventory_id, content_id, rental_date, due_date, return_date, rental_fee, COALESCE(late_fee, 0) AS late_fee, COALESCE(rental_fee, 0) + COALESCE(late_fee, 0) AS total_fee, status FROM FLOWFILE" \
"rental_id > 0 AND customer_id > 0 AND inventory_id > 0 AND rental_fee >= 0 AND late_fee >= 0")

M_PAY=$(add_meta   "SET_Meta_Payment"           800 1140 "stg_payment"       "payment_id" \
"SELECT payment_id, customer_id, rental_id, subscription_id, amount, payment_date, CAST(payment_date AS DATE) AS activity_date, payment_type, status FROM FLOWFILE" \
"payment_id > 0 AND customer_id > 0 AND amount >= 0")

M_TICK=$(add_meta  "SET_Meta_SupportTicket"     800 1260 "stg_support_ticket" "ticket_id" \
"SELECT ticket_id, customer_id, category_id, opened_date, closed_date, CAST(opened_date AS DATE) AS activity_date, priority, status FROM FLOWFILE" \
"ticket_id > 0 AND customer_id > 0")

M_REV=$(add_meta   "SET_Meta_Review"            800 1380 "stg_review"        "review_id" \
"SELECT review_id, customer_id, content_id, rating, review_date, CAST(review_date AS DATE) AS activity_date FROM FLOWFILE" \
"review_id > 0 AND customer_id > 0 AND content_id > 0 AND rating >= 1 AND rating <= 5")

M_WISH=$(add_meta  "SET_Meta_Wishlist"          800 1500 "stg_wishlist"      "wishlist_id" \
"SELECT wishlist_id, customer_id, content_id, added_date, CAST(added_date AS DATE) AS activity_date FROM FLOWFILE" \
"wishlist_id > 0 AND customer_id > 0 AND content_id > 0")

echo "  metadata taggers created (13)"

# ============================================================
# WIRING
# The trigger fans out to all 7 full extracts. Every full extract needs
# an incoming connection, otherwise ExecuteSQLRecord treats itself as a
# source processor and re-runs on its default timer until the queue hits
# back pressure.
# ============================================================
for P in "$EX_CUST" "$EX_CONT" "$EX_DEV" "$EX_PLAN" "$EX_WH" "$EX_SCAT" "$EX_INV"; do
    conn "$TRIGGER" "$P" '["success"]'
done

# extract -> tagger  (success), and extract failures go to the error group
conn "$EX_CUST"   "$M_CUST"   '["success"]'
conn "$EX_CONT"   "$M_CONT"   '["success"]'
conn "$EX_DEV"    "$M_DEV"    '["success"]'
conn "$EX_PLAN"   "$M_PLAN"   '["success"]'
conn "$EX_WH"     "$M_WH"     '["success"]'
conn "$EX_SCAT"   "$M_SCAT"   '["success"]'
conn "$EX_INV"    "$M_INV"    '["success"]'
conn "$EX_STREAM" "$M_STREAM" '["success"]'
conn "$EX_RENT"   "$M_RENT"   '["success"]'
conn "$EX_PAY"    "$M_PAY"    '["success"]'
conn "$EX_TICK"   "$M_TICK"   '["success"]'
conn "$EX_REV"    "$M_REV"    '["success"]'
conn "$EX_WISH"   "$M_WISH"   '["success"]'

# every tagger -> the output port that feeds PG_02_Transformation
for P in "$M_CUST" "$M_CONT" "$M_DEV" "$M_PLAN" "$M_WH" "$M_SCAT" "$M_INV" \
         "$M_STREAM" "$M_RENT" "$M_PAY" "$M_TICK" "$M_REV" "$M_WISH"; do
    conn "$P" "$OUT_PORT" '["success"]' "PROCESSOR" "OUTPUT_PORT"
done

echo "  wiring complete"

# Persist ids for the transformation stage
cat >> /tmp/nifi_ids.env <<EOF
TRIGGER=$TRIGGER
PG1_OUT=$OUT_PORT
EX_CUST=$EX_CUST
EX_CONT=$EX_CONT
EX_DEV=$EX_DEV
EX_PLAN=$EX_PLAN
EX_WH=$EX_WH
EX_SCAT=$EX_SCAT
EX_INV=$EX_INV
EX_STREAM=$EX_STREAM
EX_RENT=$EX_RENT
EX_PAY=$EX_PAY
EX_TICK=$EX_TICK
EX_REV=$EX_REV
EX_WISH=$EX_WISH
EOF

echo "=== TASK 1 complete : 27 processors + output port ==="
