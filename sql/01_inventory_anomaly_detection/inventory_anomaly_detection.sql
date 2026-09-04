-- ============================================================
-- PROJECT: Inventory Anomaly Detection & Alerting
-- Author : Anil Kumar Nukala
-- Domain : Warehouse Management / Supply Chain
-- Description:
--   Identifies sudden fluctuations in inventory levels,
--   flags anomalies across SKUs, and surfaces items requiring
--   immediate procurement attention.
--   Techniques: CTEs, Window Functions, CASE logic, aggregation
-- ============================================================


-- ----------------------------------------------------------------
-- STEP 1: Baseline — rolling 7-day average quantity per SKU
-- ----------------------------------------------------------------
WITH daily_inventory AS (
    SELECT
        sku_id,
        warehouse_id,
        transaction_date,
        SUM(quantity_on_hand)          AS daily_qty,
        SUM(quantity_received)         AS daily_received,
        SUM(quantity_shipped)          AS daily_shipped
    FROM warehouse.inventory_transactions
    WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE)
    GROUP BY sku_id, warehouse_id, transaction_date
),

rolling_stats AS (
    SELECT
        sku_id,
        warehouse_id,
        transaction_date,
        daily_qty,
        daily_received,
        daily_shipped,
        AVG(daily_qty) OVER (
            PARTITION BY sku_id, warehouse_id
            ORDER BY transaction_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                              AS rolling_7d_avg_qty,
        STDDEV(daily_qty) OVER (
            PARTITION BY sku_id, warehouse_id
            ORDER BY transaction_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                              AS rolling_7d_stddev,
        LAG(daily_qty, 1) OVER (
            PARTITION BY sku_id, warehouse_id
            ORDER BY transaction_date
        )                              AS prior_day_qty,
        LAG(daily_qty, 7) OVER (
            PARTITION BY sku_id, warehouse_id
            ORDER BY transaction_date
        )                              AS prior_week_qty
    FROM daily_inventory
),

-- ----------------------------------------------------------------
-- STEP 2: Compute Z-score and day-over-day change to detect spikes
-- ----------------------------------------------------------------
anomaly_scoring AS (
    SELECT
        sku_id,
        warehouse_id,
        transaction_date,
        daily_qty,
        rolling_7d_avg_qty,
        rolling_7d_stddev,
        prior_day_qty,
        prior_week_qty,

        -- Z-score: how many std deviations away from the rolling mean
        CASE
            WHEN rolling_7d_stddev = 0 OR rolling_7d_stddev IS NULL THEN 0
            ELSE ROUND((daily_qty - rolling_7d_avg_qty) / rolling_7d_stddev, 2)
        END                             AS z_score,

        -- Day-over-day percentage change
        CASE
            WHEN prior_day_qty = 0 OR prior_day_qty IS NULL THEN NULL
            ELSE ROUND(((daily_qty - prior_day_qty) * 100.0) / prior_day_qty, 2)
        END                             AS dod_pct_change,

        -- Week-over-week percentage change
        CASE
            WHEN prior_week_qty = 0 OR prior_week_qty IS NULL THEN NULL
            ELSE ROUND(((daily_qty - prior_week_qty) * 100.0) / prior_week_qty, 2)
        END                             AS wow_pct_change
    FROM rolling_stats
),

-- ----------------------------------------------------------------
-- STEP 3: Classify anomalies and assign severity
-- ----------------------------------------------------------------
anomaly_flags AS (
    SELECT
        a.*,
        p.sku_name,
        p.category,
        p.unit_cost,
        p.reorder_point,
        p.safety_stock,

        -- Anomaly severity classification
        CASE
            WHEN ABS(z_score) >= 3.0                        THEN 'CRITICAL'
            WHEN ABS(z_score) >= 2.0                        THEN 'HIGH'
            WHEN ABS(z_score) >= 1.5                        THEN 'MEDIUM'
            WHEN ABS(dod_pct_change) >= 50                  THEN 'HIGH'
            WHEN daily_qty < p.safety_stock                 THEN 'HIGH'
            WHEN daily_qty < p.reorder_point                THEN 'MEDIUM'
            ELSE 'NORMAL'
        END                             AS anomaly_severity,

        -- Human-readable anomaly reason
        CASE
            WHEN ABS(z_score) >= 3.0     THEN 'Extreme statistical deviation (Z > 3)'
            WHEN ABS(z_score) >= 2.0     THEN 'Significant deviation from 7-day average'
            WHEN daily_qty < p.safety_stock  THEN 'Below safety stock threshold'
            WHEN daily_qty < p.reorder_point THEN 'Below reorder point — replenishment needed'
            WHEN dod_pct_change <= -50   THEN 'Sudden inventory drop (>50% day-over-day)'
            WHEN dod_pct_change >= 100   THEN 'Unusual inventory spike (>100% day-over-day)'
            ELSE 'Within normal range'
        END                             AS anomaly_reason,

        -- Financial exposure of the anomaly
        ROUND(ABS(daily_qty - rolling_7d_avg_qty) * p.unit_cost, 2) AS estimated_variance_usd

    FROM anomaly_scoring a
    JOIN products.sku_master p USING (sku_id)
),

-- ----------------------------------------------------------------
-- STEP 4: Rank anomalies by financial impact within each category
-- ----------------------------------------------------------------
ranked_anomalies AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY category, transaction_date
            ORDER BY estimated_variance_usd DESC
        )                               AS rank_in_category,
        SUM(estimated_variance_usd) OVER (
            PARTITION BY warehouse_id, transaction_date
        )                               AS warehouse_total_variance_usd
    FROM anomaly_flags
    WHERE anomaly_severity != 'NORMAL'
)

-- ----------------------------------------------------------------
-- FINAL OUTPUT: Alert-ready anomaly report for today
-- ----------------------------------------------------------------
SELECT
    transaction_date,
    sku_id,
    sku_name,
    category,
    warehouse_id,
    anomaly_severity,
    anomaly_reason,
    daily_qty                           AS current_qty,
    ROUND(rolling_7d_avg_qty, 0)        AS avg_7d_qty,
    reorder_point,
    safety_stock,
    z_score,
    dod_pct_change                      AS day_over_day_pct,
    wow_pct_change                      AS week_over_week_pct,
    estimated_variance_usd,
    rank_in_category,
    warehouse_total_variance_usd
FROM ranked_anomalies
WHERE transaction_date = CURRENT_DATE
ORDER BY
    CASE anomaly_severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        ELSE 4
    END,
    estimated_variance_usd DESC;
