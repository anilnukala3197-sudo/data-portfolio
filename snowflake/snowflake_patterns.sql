-- ============================================================
-- Snowflake Ecosystem Patterns & Best Practices
-- Author: Anil Kumar Nukala
-- Description: Common Snowflake patterns used in production —
--   Time Travel, Streams, Tasks, Materialized Views, and
--   performance optimization techniques.
-- ============================================================


-- ----------------------------------------------------------------
-- 1. TIME TRAVEL — Recover from accidental data changes
-- ----------------------------------------------------------------

-- View table state from 1 hour ago
SELECT * FROM warehouse.inventory_transactions
AT (OFFSET => -3600);

-- Compare current vs. 24 hours ago to find new records
SELECT t.*
FROM warehouse.inventory_transactions             AS t
LEFT JOIN warehouse.inventory_transactions
    BEFORE (TIMESTAMP => DATEADD(hour, -24, CURRENT_TIMESTAMP)) AS hist
    ON t.transaction_id = hist.transaction_id
WHERE hist.transaction_id IS NULL;   -- records added in last 24h

-- Restore an accidentally deleted table
CREATE OR REPLACE TABLE warehouse.inventory_transactions
    CLONE warehouse.inventory_transactions
    BEFORE (STATEMENT => '<statement-id>');


-- ----------------------------------------------------------------
-- 2. STREAMS + TASKS — CDC (Change Data Capture) pipeline
-- ----------------------------------------------------------------

-- Create a stream to capture incremental changes on source table
CREATE OR REPLACE STREAM stg.inventory_changes
    ON TABLE warehouse.inventory_transactions
    APPEND_ONLY = FALSE;   -- captures inserts, updates, AND deletes

-- Task: runs every 15 min, processes only new stream records
CREATE OR REPLACE TASK etl.process_inventory_stream
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE  = '15 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('stg.inventory_changes')
AS
    MERGE INTO analytics.inventory_fact AS tgt
    USING (
        SELECT
            transaction_id,
            sku_id,
            warehouse_id,
            quantity,
            unit_cost,
            transaction_date,
            METADATA$ACTION        AS dml_action,
            METADATA$ISUPDATE      AS is_update
        FROM stg.inventory_changes
    ) AS src
    ON tgt.transaction_id = src.transaction_id
    WHEN MATCHED AND src.dml_action = 'DELETE'
        THEN DELETE
    WHEN MATCHED AND src.is_update = TRUE
        THEN UPDATE SET
            tgt.quantity         = src.quantity,
            tgt.unit_cost        = src.unit_cost,
            tgt.last_updated_ts  = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND src.dml_action = 'INSERT'
        THEN INSERT (transaction_id, sku_id, warehouse_id,
                     quantity, unit_cost, transaction_date, last_updated_ts)
             VALUES (src.transaction_id, src.sku_id, src.warehouse_id,
                     src.quantity, src.unit_cost, src.transaction_date,
                     CURRENT_TIMESTAMP());

-- Enable the task
ALTER TASK etl.process_inventory_stream RESUME;


-- ----------------------------------------------------------------
-- 3. MATERIALIZED VIEWS — Pre-aggregate for BI performance
-- ----------------------------------------------------------------

CREATE OR REPLACE MATERIALIZED VIEW analytics.mv_daily_inventory_summary
AS
SELECT
    DATE_TRUNC('day', transaction_date)    AS snapshot_day,
    warehouse_id,
    sku_id,
    category,
    SUM(CASE WHEN transaction_type = 'RECEIPT'   THEN quantity ELSE 0 END) AS total_received,
    SUM(CASE WHEN transaction_type = 'SHIPMENT'  THEN quantity ELSE 0 END) AS total_shipped,
    SUM(CASE WHEN transaction_type = 'RECEIPT'   THEN quantity ELSE 0 END)
  - SUM(CASE WHEN transaction_type = 'SHIPMENT'  THEN quantity ELSE 0 END) AS net_movement,
    AVG(unit_cost)                         AS avg_unit_cost
FROM warehouse.inventory_transactions
WHERE transaction_type IN ('RECEIPT', 'SHIPMENT')
GROUP BY 1, 2, 3, 4;

-- Query the materialized view (Snowflake auto-refreshes it)
SELECT * FROM analytics.mv_daily_inventory_summary
WHERE snapshot_day >= DATEADD(month, -3, CURRENT_DATE)
ORDER BY snapshot_day DESC;


-- ----------------------------------------------------------------
-- 4. CLUSTERING — Optimize large table scan performance
-- ----------------------------------------------------------------

-- Cluster inventory table by date and warehouse
-- (avoids full table scans in BI queries that filter on these)
ALTER TABLE warehouse.inventory_transactions
    CLUSTER BY (TO_DATE(transaction_date), warehouse_id);

-- Check clustering depth (lower = better partitioning)
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'warehouse.inventory_transactions',
    '(TO_DATE(transaction_date), warehouse_id)'
);


-- ----------------------------------------------------------------
-- 5. DYNAMIC DATA MASKING — Protect PII in shared schemas
-- ----------------------------------------------------------------

-- Create a masking policy for vendor contact info
CREATE OR REPLACE MASKING POLICY pii.mask_vendor_email
    AS (email_val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'FINANCE_ANALYST')
            THEN email_val
        ELSE REGEXP_REPLACE(email_val, '.+@', '****@')
    END;

-- Apply masking policy to the vendor table
ALTER TABLE erp.vendors
    MODIFY COLUMN contact_email
    SET MASKING POLICY pii.mask_vendor_email;


-- ----------------------------------------------------------------
-- 6. QUERY PERFORMANCE — Optimization patterns
-- ----------------------------------------------------------------

-- Use QUALIFY instead of a subquery for window-function filtering
-- (faster in Snowflake — avoids wrapping in a CTE/subquery)
SELECT
    supplier_id,
    invoice_number,
    gross_amount,
    ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY gross_amount DESC) AS rn
FROM erp.general_ledger_invoices
QUALIFY rn = 1;   -- top invoice per supplier, no subquery needed

-- FLATTEN JSON semi-structured column
SELECT
    order_id,
    f.value:sku_id::STRING       AS sku_id,
    f.value:quantity::NUMBER     AS quantity,
    f.value:unit_price::FLOAT    AS unit_price
FROM orders.raw_order_lines,
LATERAL FLATTEN(INPUT => line_items_json) f;

-- Use RESULT_SCAN to chain query results without re-running
-- (useful in stored procedures and multi-step ETL)
SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


-- ----------------------------------------------------------------
-- 7. STORED PROCEDURE — Parameterized monthly close process
-- ----------------------------------------------------------------

CREATE OR REPLACE PROCEDURE finance.run_monthly_close(
    p_year   NUMBER,
    p_month  NUMBER
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS $$
    var period = P_YEAR + '-' + String(P_MONTH).padStart(2, '0');

    // Step 1: Refresh staging aggregates
    var stmt1 = snowflake.execute({
        sqlText: `
            INSERT INTO finance.monthly_close_staging
            SELECT sku_id, warehouse_id,
                   SUM(quantity) AS net_qty,
                   SUM(quantity * unit_cost) AS total_value,
                   '${period}' AS close_period
            FROM warehouse.inventory_transactions
            WHERE TO_CHAR(transaction_date, 'YYYY-MM') = '${period}'
            GROUP BY sku_id, warehouse_id
        `
    });

    // Step 2: Upsert into final close table
    var stmt2 = snowflake.execute({
        sqlText: `
            MERGE INTO finance.monthly_close AS tgt
            USING finance.monthly_close_staging AS src
                ON tgt.sku_id = src.sku_id
               AND tgt.warehouse_id = src.warehouse_id
               AND tgt.close_period = src.close_period
            WHEN MATCHED THEN UPDATE SET
                tgt.net_qty    = src.net_qty,
                tgt.total_value = src.total_value,
                tgt.updated_at  = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (sku_id, warehouse_id, close_period, net_qty, total_value, updated_at)
                VALUES (src.sku_id, src.warehouse_id, src.close_period,
                        src.net_qty, src.total_value, CURRENT_TIMESTAMP())
        `
    });

    return 'Monthly close for ' + period + ' completed successfully.';
$$;

-- Execute the procedure
CALL finance.run_monthly_close(2025, 8);
