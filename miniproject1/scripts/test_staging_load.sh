#!/usr/bin/env bash
# ============================================================
# SQL VALIDATION HARNESS  (development aid - not part of the deliverable pipeline)
#
# Apache NiFi is the ETL tool for this project. This script exists
# only to prove the staging -> dimension -> fact SQL is correct
# BEFORE it is embedded in NiFi processors, by moving the same rows
# with psql COPY instead of NiFi record processors.
#
# Usage:  bash scripts/test_staging_load.sh
# ============================================================
set -e

PSQL="/c/pgsql/bin/psql"
OPER="-U postgres -d abc_hub_operational"
ANAL="-U postgres -d abc_hub_analytical"

copy_table () {
    local target="$1"; local collist="$2"; local query="$3"
    echo "  -> $target"
    $PSQL $ANAL -q -c "TRUNCATE $target;"
    $PSQL $OPER -q -c "COPY ($query) TO STDOUT WITH (FORMAT csv)" \
      | $PSQL $ANAL -q -c "COPY $target ($collist) FROM STDIN WITH (FORMAT csv)"
}

echo "STEP 1 - populate staging from operational"

copy_table "staging.stg_customer" "customer_id, customer_no, first_name, last_name, email, phone, date_of_birth, gender, registration_date, status, city_name, country_name, plan_name, monthly_fee" "
SELECT c.customer_id, c.customer_no, c.first_name, c.last_name, c.email, c.phone,
       c.date_of_birth, c.gender, c.registration_date, c.status,
       ci.city_name, co.country_name, sp.plan_name, sp.monthly_fee
FROM customer c
LEFT JOIN customer_address ca ON ca.customer_id = c.customer_id AND ca.address_type = 'home'
LEFT JOIN city ci ON ci.city_id = ca.city_id
LEFT JOIN country co ON co.country_id = ci.country_id
LEFT JOIN customer_subscription cs ON cs.customer_id = c.customer_id AND cs.status = 'active'
LEFT JOIN subscription_plan sp ON sp.plan_id = cs.plan_id"

copy_table "staging.stg_content" "content_id, title, content_type, release_date, duration_minutes, language, age_rating, primary_genre" "
SELECT c.content_id, c.title, ct.content_type, c.release_date, c.duration_minutes,
       c.language, c.age_rating,
       (SELECT MIN(g.genre_name) FROM content_genre cg JOIN genre g ON g.genre_id = cg.genre_id
        WHERE cg.content_id = c.content_id)
FROM content c JOIN content_type ct ON ct.content_type_id = c.content_type_id"

copy_table "staging.stg_device" "device_id, device_name" \
  "SELECT device_id, device_name FROM device"

copy_table "staging.stg_subscription_plan" "plan_id, plan_name, monthly_fee, video_quality, max_devices" \
  "SELECT plan_id, plan_name, monthly_fee, video_quality, max_devices FROM subscription_plan"

copy_table "staging.stg_warehouse" "warehouse_id, warehouse_name, city_name, country_name" "
SELECT w.warehouse_id, w.warehouse_name, ci.city_name, co.country_name
FROM warehouse w JOIN city ci ON ci.city_id = w.city_id
JOIN country co ON co.country_id = ci.country_id"

copy_table "staging.stg_support_category" "category_id, category_name" \
  "SELECT category_id, category_name FROM support_category"

copy_table "staging.stg_inventory_item" "inventory_id, content_id, content_title, content_type, warehouse_id, warehouse_name, city_name, barcode, purchase_date, item_condition, status" "
SELECT i.inventory_id, i.content_id, c.title, ct.content_type, i.warehouse_id,
       w.warehouse_name, ci.city_name, i.barcode, i.purchase_date, i.item_condition, i.status
FROM inventory_item i
JOIN content c ON c.content_id = i.content_id
JOIN content_type ct ON ct.content_type_id = c.content_type_id
JOIN warehouse w ON w.warehouse_id = i.warehouse_id
JOIN city ci ON ci.city_id = w.city_id"

copy_table "staging.stg_streaming_session" "stream_id, customer_id, content_id, device_id, start_time, end_time, activity_date, watch_duration, completion_percentage" "
SELECT stream_id, customer_id, content_id, device_id, start_time, end_time,
       CAST(start_time AS DATE), COALESCE(watch_duration,0), COALESCE(completion_percentage,0)
FROM streaming_session"

copy_table "staging.stg_rental" "rental_id, customer_id, inventory_id, content_id, rental_date, due_date, return_date, rental_fee, late_fee, total_fee, status" "
SELECT r.rental_id, r.customer_id, r.inventory_id, i.content_id, r.rental_date, r.due_date,
       r.return_date, r.rental_fee, COALESCE(r.late_fee,0),
       r.rental_fee + COALESCE(r.late_fee,0), r.status
FROM rental r JOIN inventory_item i ON i.inventory_id = r.inventory_id"

copy_table "staging.stg_payment" "payment_id, customer_id, rental_id, subscription_id, amount, payment_date, activity_date, payment_type, status" "
SELECT payment_id, customer_id, rental_id, subscription_id, amount, payment_date,
       CAST(payment_date AS DATE), payment_type, status FROM payment"

copy_table "staging.stg_support_ticket" "ticket_id, customer_id, category_id, opened_date, closed_date, activity_date, priority, status" "
SELECT ticket_id, customer_id, category_id, opened_date, closed_date,
       CAST(opened_date AS DATE), priority, status FROM support_ticket"

copy_table "staging.stg_review" "review_id, customer_id, content_id, rating, review_date, activity_date" "
SELECT review_id, customer_id, content_id, rating, review_date,
       CAST(review_date AS DATE) FROM review"

copy_table "staging.stg_wishlist" "wishlist_id, customer_id, content_id, added_date, activity_date" "
SELECT wishlist_id, customer_id, content_id, added_date,
       CAST(added_date AS DATE) FROM wishlist"

echo "STEP 2 - load dimensions (same procedure calls NiFi makes, same order)"
$PSQL $ANAL -q -c "CALL staging.sp_load_dim_reference();"
$PSQL $ANAL -q -c "CALL staging.sp_load_dim_customer();"
$PSQL $ANAL -q -c "CALL staging.sp_load_dim_content();"
$PSQL $ANAL -q -c "CALL staging.sp_load_dim_inventory();"

echo "STEP 3 - load facts (only after every dimension is present)"
$PSQL $ANAL -q -c "CALL staging.sp_load_fact_customer_daily();"
$PSQL $ANAL -q -c "CALL staging.sp_load_fact_content_monthly();"
$PSQL $ANAL -q -c "CALL staging.sp_load_fact_inventory_util();"

echo "STEP 4 - row counts"
$PSQL $ANAL -c "
SELECT 'dim_customer' t, COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_content',            COUNT(*) FROM dim_content
UNION ALL SELECT 'dim_device',             COUNT(*) FROM dim_device
UNION ALL SELECT 'dim_inventory_item',     COUNT(*) FROM dim_inventory_item
UNION ALL SELECT 'dim_warehouse',          COUNT(*) FROM dim_warehouse
UNION ALL SELECT 'fact_customer_daily',    COUNT(*) FROM fact_customer_daily_activity
UNION ALL SELECT 'fact_content_monthly',   COUNT(*) FROM fact_content_monthly_performance
UNION ALL SELECT 'fact_inventory_util',    COUNT(*) FROM fact_inventory_utilisation;"
