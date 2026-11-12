-- ============================================================
-- BRONZE EXTRACT - Inventory domain
-- Task: extract_inventory  (Extract stage, Airflow TaskGroup)
-- Same full-extract-as-of-{{ data_interval_end }} + upsert pattern as
-- 01_extract_reference.sql (see that file's header for why this is a
-- full extract, not a windowed one - this is exactly the table whose
-- missing history caused the referential-integrity rejects on rental
-- rows before this fix).
-- ============================================================

INSERT INTO bronze.raw_inventory_item (
    inventory_id, content_id, warehouse_id, barcode, purchase_date,
    item_condition, status, created_at, updated_at, batch_id
)
SELECT
    inventory_id, content_id, warehouse_id, barcode, purchase_date,
    item_condition, status, created_at, updated_at, '{{ run_id }}'
FROM source.inventory_item
WHERE created_at < '{{ data_interval_end }}' OR updated_at < '{{ data_interval_end }}'
ON CONFLICT (inventory_id) DO UPDATE SET
    content_id = EXCLUDED.content_id, warehouse_id = EXCLUDED.warehouse_id,
    barcode = EXCLUDED.barcode, purchase_date = EXCLUDED.purchase_date,
    item_condition = EXCLUDED.item_condition, status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id, extracted_at = CURRENT_TIMESTAMP;
