-- ============================================================
-- PROJECT: Procurement Supplier Performance Scorecard
-- Author : Anil Kumar Nukala
-- Domain : Procurement / Supply Chain Analytics
-- Description:
--   Multi-dimensional supplier scorecard tracking delivery
--   performance, invoice accuracy, pricing variance, and
--   quality metrics across rolling time periods.
--   Techniques: CTEs, PIVOT, window functions, CASE scoring,
--               correlated subqueries, period-over-period
-- ============================================================


-- ----------------------------------------------------------------
-- STEP 1: Pull base purchase order data
-- ----------------------------------------------------------------
WITH po_base AS (
    SELECT
        po.po_id,
        po.supplier_id,
        po.sku_id,
        po.po_date,
        po.promised_delivery_date,
        po.actual_delivery_date,
        po.ordered_qty,
        po.received_qty,
        po.unit_price_ordered,
        po.unit_price_invoiced,
        po.invoice_id,
        po.line_status,
        DATEDIFF(day, po.promised_delivery_date, po.actual_delivery_date) AS delivery_delay_days,
        CASE WHEN po.actual_delivery_date <= po.promised_delivery_date
             THEN 1 ELSE 0 END                                             AS is_on_time
    FROM procurement.purchase_orders po
    WHERE po.po_date >= DATEADD(month, -12, CURRENT_DATE)
      AND po.line_status IN ('RECEIVED', 'INVOICED', 'CLOSED')
),

-- ----------------------------------------------------------------
-- STEP 2: Invoice accuracy — compare ordered vs invoiced price
-- ----------------------------------------------------------------
invoice_accuracy AS (
    SELECT
        supplier_id,
        COUNT(*)                                            AS total_line_items,
        SUM(CASE WHEN ABS(unit_price_invoiced - unit_price_ordered)
                      / NULLIF(unit_price_ordered, 0) <= 0.01
                 THEN 1 ELSE 0 END)                        AS accurate_invoice_lines,
        ROUND(AVG(
            (unit_price_invoiced - unit_price_ordered)
            / NULLIF(unit_price_ordered, 0) * 100
        ), 2)                                              AS avg_price_variance_pct,
        SUM((unit_price_invoiced - unit_price_ordered)
            * received_qty)                                AS total_invoice_variance_usd
    FROM po_base
    GROUP BY supplier_id
),

-- ----------------------------------------------------------------
-- STEP 3: Delivery performance aggregation
-- ----------------------------------------------------------------
delivery_perf AS (
    SELECT
        supplier_id,
        COUNT(DISTINCT po_id)                              AS total_pos,
        SUM(ordered_qty)                                   AS total_ordered_qty,
        SUM(received_qty)                                  AS total_received_qty,
        ROUND(SUM(received_qty) * 100.0
              / NULLIF(SUM(ordered_qty), 0), 2)            AS fill_rate_pct,
        ROUND(AVG(is_on_time) * 100, 2)                    AS on_time_delivery_pct,
        ROUND(AVG(CASE WHEN delivery_delay_days > 0
                       THEN delivery_delay_days END), 1)   AS avg_delay_days_when_late,
        MAX(delivery_delay_days)                           AS max_delay_days
    FROM po_base
    GROUP BY supplier_id
),

-- ----------------------------------------------------------------
-- STEP 4: Quality metrics from goods receipt notes
-- ----------------------------------------------------------------
quality_metrics AS (
    SELECT
        grn.supplier_id,
        COUNT(*)                                           AS total_receipts,
        SUM(grn.rejected_qty)                              AS total_rejected_qty,
        SUM(grn.accepted_qty)                              AS total_accepted_qty,
        ROUND(SUM(grn.rejected_qty) * 100.0
              / NULLIF(SUM(grn.received_qty), 0), 2)       AS defect_rate_pct,
        COUNT(DISTINCT CASE WHEN grn.rejected_qty > 0
                            THEN grn.grn_id END)           AS shipments_with_defects
    FROM procurement.goods_receipt_notes grn
    WHERE grn.receipt_date >= DATEADD(month, -12, CURRENT_DATE)
    GROUP BY grn.supplier_id
),

-- ----------------------------------------------------------------
-- STEP 5: Period-over-period — current quarter vs prior quarter
-- ----------------------------------------------------------------
current_qtr AS (
    SELECT supplier_id,
           ROUND(AVG(is_on_time) * 100, 2) AS otd_pct_current_qtr
    FROM po_base
    WHERE po_date >= DATE_TRUNC('quarter', CURRENT_DATE)
    GROUP BY supplier_id
),

prior_qtr AS (
    SELECT supplier_id,
           ROUND(AVG(is_on_time) * 100, 2) AS otd_pct_prior_qtr
    FROM po_base
    WHERE po_date >= DATEADD(month, -6, DATE_TRUNC('quarter', CURRENT_DATE))
      AND po_date <  DATE_TRUNC('quarter', CURRENT_DATE)
    GROUP BY supplier_id
),

-- ----------------------------------------------------------------
-- STEP 6: Composite supplier score (0-100)
-- ----------------------------------------------------------------
composite_score AS (
    SELECT
        d.supplier_id,
        s.supplier_name,
        s.supplier_tier,
        s.country_of_origin,
        d.total_pos,
        d.fill_rate_pct,
        d.on_time_delivery_pct,
        d.avg_delay_days_when_late,
        i.avg_price_variance_pct,
        i.total_invoice_variance_usd,
        COALESCE(q.defect_rate_pct, 0)          AS defect_rate_pct,
        COALESCE(q.shipments_with_defects, 0)   AS shipments_with_defects,
        cq.otd_pct_current_qtr,
        pq.otd_pct_prior_qtr,
        cq.otd_pct_current_qtr
            - COALESCE(pq.otd_pct_prior_qtr, cq.otd_pct_current_qtr)
                                                AS otd_qtr_delta,

        -- Weighted score: OTD 35% | Fill Rate 25% | Invoice Accuracy 20% | Quality 20%
        ROUND(
            (d.on_time_delivery_pct * 0.35)
          + (d.fill_rate_pct        * 0.25)
          + (GREATEST(0, 100 - ABS(i.avg_price_variance_pct) * 10) * 0.20)
          + (GREATEST(0, 100 - COALESCE(q.defect_rate_pct, 0) * 5) * 0.20)
        , 1)                                    AS composite_score
    FROM delivery_perf d
    JOIN procurement.suppliers s   USING (supplier_id)
    LEFT JOIN invoice_accuracy i   USING (supplier_id)
    LEFT JOIN quality_metrics  q   USING (supplier_id)
    LEFT JOIN current_qtr      cq  USING (supplier_id)
    LEFT JOIN prior_qtr        pq  USING (supplier_id)
),

-- ----------------------------------------------------------------
-- STEP 7: Rank suppliers within tier
-- ----------------------------------------------------------------
final_ranked AS (
    SELECT
        *,
        CASE
            WHEN composite_score >= 90 THEN 'PREFERRED'
            WHEN composite_score >= 75 THEN 'APPROVED'
            WHEN composite_score >= 60 THEN 'CONDITIONAL'
            ELSE                            'AT RISK'
        END                             AS supplier_status,
        RANK() OVER (
            PARTITION BY supplier_tier
            ORDER BY composite_score DESC
        )                               AS rank_within_tier,
        NTILE(4) OVER (
            ORDER BY composite_score DESC
        )                               AS performance_quartile
    FROM composite_score
)

-- ----------------------------------------------------------------
-- FINAL OUTPUT: Executive supplier scorecard
-- ----------------------------------------------------------------
SELECT
    supplier_id,
    supplier_name,
    supplier_tier,
    country_of_origin,
    total_pos,
    fill_rate_pct,
    on_time_delivery_pct,
    avg_delay_days_when_late,
    defect_rate_pct,
    avg_price_variance_pct,
    ROUND(total_invoice_variance_usd, 2)    AS invoice_variance_usd,
    otd_pct_current_qtr,
    otd_pct_prior_qtr,
    otd_qtr_delta,
    composite_score,
    supplier_status,
    rank_within_tier,
    CASE performance_quartile
        WHEN 1 THEN 'Top 25%'
        WHEN 2 THEN 'Upper Mid'
        WHEN 3 THEN 'Lower Mid'
        WHEN 4 THEN 'Bottom 25%'
    END                                     AS performance_quartile
FROM final_ranked
ORDER BY composite_score DESC;
