-- ============================================================
-- PROJECT: Multi-Source Financial Data Reconciliation
-- Author : Anil Kumar Nukala
-- Domain : Finance / Accounting / Data Quality
-- Description:
--   Reconciles invoice data between the WMS (Warehouse Mgmt
--   System) and the ERP General Ledger, flags discrepancies,
--   classifies variance types, and generates an audit-ready
--   exception report.
--   Techniques: FULL OUTER JOIN, COALESCE, CASE, CTEs,
--               recursive discrepancy bucketing, SCD logic
-- ============================================================


-- ----------------------------------------------------------------
-- STEP 1: Pull WMS invoice summary (source of truth for shipments)
-- ----------------------------------------------------------------
WITH wms_invoices AS (
    SELECT
        invoice_number,
        vendor_id,
        po_number,
        invoice_date,
        due_date,
        SUM(line_amount)        AS wms_total_amount,
        SUM(tax_amount)         AS wms_tax_amount,
        SUM(line_amount)
            + SUM(tax_amount)   AS wms_gross_amount,
        COUNT(*)                AS wms_line_count,
        MAX(posting_status)     AS wms_posting_status
    FROM warehouse.ap_invoices
    WHERE invoice_date BETWEEN :start_date AND :end_date
      AND posting_status != 'VOIDED'
    GROUP BY invoice_number, vendor_id, po_number, invoice_date, due_date
),

-- ----------------------------------------------------------------
-- STEP 2: Pull ERP/GL invoice summary
-- ----------------------------------------------------------------
erp_invoices AS (
    SELECT
        invoice_number,
        vendor_id,
        po_number,
        gl_posting_date         AS invoice_date,
        payment_due_date        AS due_date,
        SUM(net_amount)         AS erp_total_amount,
        SUM(tax_amount)         AS erp_tax_amount,
        SUM(gross_amount)       AS erp_gross_amount,
        COUNT(*)                AS erp_line_count,
        MAX(approval_status)    AS erp_approval_status,
        MAX(payment_status)     AS erp_payment_status
    FROM erp.general_ledger_invoices
    WHERE gl_posting_date BETWEEN :start_date AND :end_date
      AND approval_status NOT IN ('REJECTED', 'CANCELLED')
    GROUP BY invoice_number, vendor_id, po_number,
             gl_posting_date, payment_due_date
),

-- ----------------------------------------------------------------
-- STEP 3: Full outer join to surface all discrepancy types
-- ----------------------------------------------------------------
reconciled AS (
    SELECT
        COALESCE(w.invoice_number,  e.invoice_number)   AS invoice_number,
        COALESCE(w.vendor_id,       e.vendor_id)         AS vendor_id,
        COALESCE(w.po_number,       e.po_number)         AS po_number,
        COALESCE(w.invoice_date,    e.invoice_date)      AS invoice_date,
        w.wms_total_amount,
        w.wms_tax_amount,
        w.wms_gross_amount,
        w.wms_line_count,
        w.wms_posting_status,
        e.erp_total_amount,
        e.erp_tax_amount,
        e.erp_gross_amount,
        e.erp_line_count,
        e.erp_approval_status,
        e.erp_payment_status,

        -- Presence flags
        CASE WHEN w.invoice_number IS NOT NULL THEN 1 ELSE 0 END AS in_wms,
        CASE WHEN e.invoice_number IS NOT NULL THEN 1 ELSE 0 END AS in_erp,

        -- Variance calculations
        COALESCE(e.erp_gross_amount, 0)
            - COALESCE(w.wms_gross_amount, 0)            AS gross_amount_variance,
        COALESCE(e.erp_tax_amount, 0)
            - COALESCE(w.wms_tax_amount, 0)              AS tax_variance,
        COALESCE(e.erp_line_count, 0)
            - COALESCE(w.wms_line_count, 0)              AS line_count_variance

    FROM wms_invoices  w
    FULL OUTER JOIN erp_invoices e
        ON  w.invoice_number = e.invoice_number
        AND w.vendor_id      = e.vendor_id
),

-- ----------------------------------------------------------------
-- STEP 4: Classify discrepancy type and severity
-- ----------------------------------------------------------------
classified AS (
    SELECT
        r.*,
        v.vendor_name,
        v.vendor_category,
        v.payment_terms,

        -- Discrepancy type
        CASE
            WHEN in_wms = 1 AND in_erp = 0
                THEN 'MISSING_IN_ERP'
            WHEN in_wms = 0 AND in_erp = 1
                THEN 'MISSING_IN_WMS'
            WHEN in_wms = 1 AND in_erp = 1
                 AND ABS(gross_amount_variance) > 0.01
                THEN 'AMOUNT_MISMATCH'
            WHEN in_wms = 1 AND in_erp = 1
                 AND line_count_variance != 0
                THEN 'LINE_COUNT_MISMATCH'
            WHEN in_wms = 1 AND in_erp = 1
                 AND ABS(tax_variance) > 0.01
                THEN 'TAX_VARIANCE'
            ELSE 'MATCHED'
        END                                             AS discrepancy_type,

        -- Variance severity bucket
        CASE
            WHEN ABS(gross_amount_variance) >= 10000    THEN 'HIGH'
            WHEN ABS(gross_amount_variance) >= 1000     THEN 'MEDIUM'
            WHEN ABS(gross_amount_variance) > 0.01      THEN 'LOW'
            ELSE                                             'NONE'
        END                                             AS variance_severity,

        -- Variance as percentage of WMS amount
        CASE
            WHEN wms_gross_amount != 0 AND wms_gross_amount IS NOT NULL
            THEN ROUND(
                gross_amount_variance * 100.0 / wms_gross_amount
            , 2)
            ELSE NULL
        END                                             AS variance_pct

    FROM reconciled r
    LEFT JOIN erp.vendors v ON r.vendor_id = v.vendor_id
),

-- ----------------------------------------------------------------
-- STEP 5: Running totals by vendor for audit summary
-- ----------------------------------------------------------------
vendor_summary AS (
    SELECT
        vendor_id,
        vendor_name,
        vendor_category,
        COUNT(*)                                        AS total_invoices,
        SUM(CASE WHEN discrepancy_type = 'MATCHED'
                 THEN 1 ELSE 0 END)                    AS matched_count,
        SUM(CASE WHEN discrepancy_type != 'MATCHED'
                 THEN 1 ELSE 0 END)                    AS exception_count,
        ROUND(SUM(CASE WHEN discrepancy_type != 'MATCHED'
                       THEN 1 ELSE 0 END) * 100.0
              / NULLIF(COUNT(*), 0), 2)                AS exception_rate_pct,
        SUM(ABS(gross_amount_variance))                AS total_abs_variance_usd
    FROM classified
    GROUP BY vendor_id, vendor_name, vendor_category
)

-- ----------------------------------------------------------------
-- FINAL OUTPUT 1: Exception report (exceptions only)
-- ----------------------------------------------------------------
SELECT
    c.invoice_number,
    c.vendor_id,
    c.vendor_name,
    c.vendor_category,
    c.po_number,
    c.invoice_date,
    c.discrepancy_type,
    c.variance_severity,
    c.wms_gross_amount,
    c.erp_gross_amount,
    c.gross_amount_variance,
    c.variance_pct,
    c.tax_variance,
    c.line_count_variance,
    c.wms_posting_status,
    c.erp_approval_status,
    c.erp_payment_status,
    vs.exception_rate_pct           AS vendor_exception_rate,
    vs.total_abs_variance_usd       AS vendor_total_variance_usd
FROM classified c
JOIN vendor_summary vs USING (vendor_id)
WHERE c.discrepancy_type != 'MATCHED'
ORDER BY
    CASE c.variance_severity
        WHEN 'HIGH'   THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW'    THEN 3
        ELSE 4
    END,
    ABS(c.gross_amount_variance) DESC;


-- ----------------------------------------------------------------
-- FINAL OUTPUT 2: Executive reconciliation summary by vendor
-- ----------------------------------------------------------------
/*
SELECT
    vendor_id,
    vendor_name,
    vendor_category,
    total_invoices,
    matched_count,
    exception_count,
    exception_rate_pct,
    ROUND(total_abs_variance_usd, 2) AS total_abs_variance_usd
FROM vendor_summary
ORDER BY total_abs_variance_usd DESC;
*/
