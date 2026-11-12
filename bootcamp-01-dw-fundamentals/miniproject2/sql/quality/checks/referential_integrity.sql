-- SQLCheckOperator ("Data Validation" stage): every rental whose
-- inventory_id does not resolve in Bronze was quarantined.
SELECT COUNT(*) = 0
FROM bronze.raw_rental r
WHERE r.batch_id = '{{ run_id }}'
  AND NOT EXISTS (SELECT 1 FROM bronze.raw_inventory_item i WHERE i.inventory_id = r.inventory_id)
  AND NOT EXISTS (SELECT 1 FROM silver.dq_rejected rej
                   WHERE rej.source_table = 'bronze.raw_rental'
                     AND rej.source_pk = r.rental_id::TEXT AND rej.batch_id = r.batch_id);
