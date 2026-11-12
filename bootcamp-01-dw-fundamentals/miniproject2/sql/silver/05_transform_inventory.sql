-- ============================================================
-- SILVER TRANSFORM - Inventory domain
-- Task: transform_inventory  (Silver Transformation stage)
-- Denormalises content title/type and holding warehouse/city
-- against Bronze, same shape Mini Project 01's stg_inventory_item
-- carried.
-- ============================================================

INSERT INTO silver.inventory_item (
    inventory_id, content_id, content_title, content_type,
    warehouse_id, warehouse_name, city_name, barcode, purchase_date,
    item_condition, status, batch_id
)
SELECT
    i.inventory_id, i.content_id, c.title, ct.content_type,
    i.warehouse_id, w.warehouse_name, ci.city_name, i.barcode, i.purchase_date,
    i.item_condition, INITCAP(TRIM(i.status)), i.batch_id
FROM bronze.raw_inventory_item i
JOIN bronze.raw_content      c  ON c.content_id       = i.content_id
JOIN bronze.raw_content_type ct ON ct.content_type_id = c.content_type_id
JOIN bronze.raw_warehouse    w  ON w.warehouse_id     = i.warehouse_id
JOIN bronze.raw_city         ci ON ci.city_id         = w.city_id
WHERE i.batch_id = '{{ run_id }}'
ON CONFLICT (inventory_id) DO UPDATE SET
    content_id = EXCLUDED.content_id, content_title = EXCLUDED.content_title,
    content_type = EXCLUDED.content_type, warehouse_id = EXCLUDED.warehouse_id,
    warehouse_name = EXCLUDED.warehouse_name, city_name = EXCLUDED.city_name,
    barcode = EXCLUDED.barcode, purchase_date = EXCLUDED.purchase_date,
    item_condition = EXCLUDED.item_condition, status = EXCLUDED.status,
    batch_id = EXCLUDED.batch_id, load_timestamp = CURRENT_TIMESTAMP;
