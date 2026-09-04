"""
============================================================
PROJECT: End-to-End ETL Pipeline — WMS to Snowflake
Author : Anil Kumar Nukala
Domain : Warehouse Management / Data Engineering
Description:
    Extracts inventory and shipment data from a SQL Server
    WMS database, applies business transformation rules,
    and loads the cleansed data into Snowflake for BI
    consumption. Includes logging, error handling, and
    incremental load support.
============================================================
"""

import os
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pyodbc
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# ----------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(f"logs/etl_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ----------------------------------------------------------------
# Connection helpers
# ----------------------------------------------------------------
def get_wms_connection() -> pyodbc.Connection:
    """Return a connection to the WMS SQL Server database."""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('WMS_SERVER')};"
        f"DATABASE={os.getenv('WMS_DATABASE')};"
        f"UID={os.getenv('WMS_USER')};"
        f"PWD={os.getenv('WMS_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=30)


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Return a connection to the Snowflake data warehouse."""
    return snowflake.connector.connect(
        account=os.getenv("SF_ACCOUNT"),
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SF_DATABASE"),
        schema=os.getenv("SF_SCHEMA", "STAGING"),
        role=os.getenv("SF_ROLE", "TRANSFORMER"),
    )


# ----------------------------------------------------------------
# Extract
# ----------------------------------------------------------------
def extract_inventory_transactions(
    wms_conn: pyodbc.Connection,
    incremental_from: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Pull inventory transactions from WMS.
    Supports full load (incremental_from=None) or
    incremental load from a given timestamp.
    """
    watermark = incremental_from or (datetime.now() - timedelta(days=90))
    log.info(f"Extracting WMS transactions since {watermark:%Y-%m-%d %H:%M}")

    query = """
        SELECT
            t.TransactionID         AS transaction_id,
            t.SKU_ID                AS sku_id,
            t.WarehouseID           AS warehouse_id,
            t.TransactionType       AS transaction_type,
            t.Quantity              AS quantity,
            t.UnitCost              AS unit_cost,
            t.TransactionDate       AS transaction_date,
            t.ReferenceNumber       AS reference_number,
            t.CreatedBy             AS created_by,
            t.ModifiedDate          AS modified_date,
            s.SKUName               AS sku_name,
            s.Category              AS category,
            s.UOM                   AS unit_of_measure,
            w.WarehouseName         AS warehouse_name,
            w.Region                AS region
        FROM dbo.InventoryTransactions t
        JOIN dbo.SKUMaster             s ON t.SKU_ID      = s.SKU_ID
        JOIN dbo.Warehouses            w ON t.WarehouseID = w.WarehouseID
        WHERE t.ModifiedDate >= ?
          AND t.TransactionStatus != 'VOIDED'
        ORDER BY t.TransactionDate
    """
    df = pd.read_sql(query, wms_conn, params=[watermark])
    log.info(f"Extracted {len(df):,} rows from WMS")
    return df


# ----------------------------------------------------------------
# Transform
# ----------------------------------------------------------------
def transform_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply business transformation rules to raw WMS data.
    Returns a cleansed DataFrame ready for Snowflake load.
    """
    log.info("Applying transformations ...")

    # 1. Normalize column names to Snowflake convention (UPPER_SNAKE)
    df.columns = [c.upper() for c in df.columns]

    # 2. Parse and standardize dates
    df["TRANSACTION_DATE"] = pd.to_datetime(df["TRANSACTION_DATE"], errors="coerce")
    df["MODIFIED_DATE"]    = pd.to_datetime(df["MODIFIED_DATE"],    errors="coerce")
    df["LOAD_TIMESTAMP"]   = datetime.utcnow()

    # 3. Drop rows where quantity or transaction_date is null (bad records)
    before = len(df)
    df = df.dropna(subset=["QUANTITY", "TRANSACTION_DATE"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"Dropped {dropped} rows with null QUANTITY or TRANSACTION_DATE")

    # 4. Standardize transaction type codes
    type_map = {
        "RCPT": "RECEIPT",
        "SHIP": "SHIPMENT",
        "ADJ+": "ADJUSTMENT_POSITIVE",
        "ADJ-": "ADJUSTMENT_NEGATIVE",
        "TRFR": "TRANSFER",
        "RTN" : "RETURN",
    }
    df["TRANSACTION_TYPE"] = (
        df["TRANSACTION_TYPE"].str.strip().str.upper().map(type_map)
        .fillna(df["TRANSACTION_TYPE"])
    )

    # 5. Derive signed quantity (receipts positive, shipments negative)
    df["SIGNED_QUANTITY"] = df.apply(
        lambda r: -abs(r["QUANTITY"]) if r["TRANSACTION_TYPE"] in ("SHIPMENT", "ADJUSTMENT_NEGATIVE")
                   else abs(r["QUANTITY"]),
        axis=1,
    )

    # 6. Compute extended cost
    df["EXTENDED_COST"] = (df["QUANTITY"] * df["UNIT_COST"]).round(2)

    # 7. Derive calendar fields for partition pruning
    df["TRANSACTION_YEAR"]  = df["TRANSACTION_DATE"].dt.year
    df["TRANSACTION_MONTH"] = df["TRANSACTION_DATE"].dt.month
    df["TRANSACTION_WEEK"]  = df["TRANSACTION_DATE"].dt.isocalendar().week.astype(int)

    # 8. Generate a deterministic row hash for deduplication
    hash_cols = ["TRANSACTION_ID", "SKU_ID", "WAREHOUSE_ID",
                 "TRANSACTION_DATE", "QUANTITY"]
    df["ROW_HASH"] = df[hash_cols].astype(str).agg("|".join, axis=1).apply(
        lambda s: hashlib.md5(s.encode()).hexdigest()
    )

    # 9. Clean text fields
    for col in ["SKU_NAME", "CATEGORY", "WAREHOUSE_NAME", "REGION"]:
        df[col] = df[col].str.strip().str.title()

    log.info(f"Transformation complete — {len(df):,} clean rows ready")
    return df


# ----------------------------------------------------------------
# Load
# ----------------------------------------------------------------
def load_to_snowflake(
    df: pd.DataFrame,
    sf_conn: snowflake.connector.SnowflakeConnection,
    target_table: str = "STG_INVENTORY_TRANSACTIONS",
) -> int:
    """
    Load the transformed DataFrame into Snowflake using
    the high-speed write_pandas connector.
    Returns the number of rows written.
    """
    log.info(f"Loading {len(df):,} rows into Snowflake {target_table} ...")

    # Ensure target table exists (idempotent DDL)
    create_ddl = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            TRANSACTION_ID        VARCHAR(50),
            SKU_ID                VARCHAR(50),
            WAREHOUSE_ID          VARCHAR(50),
            TRANSACTION_TYPE      VARCHAR(50),
            QUANTITY              NUMBER(18, 4),
            SIGNED_QUANTITY       NUMBER(18, 4),
            UNIT_COST             NUMBER(18, 4),
            EXTENDED_COST         NUMBER(18, 2),
            TRANSACTION_DATE      TIMESTAMP_NTZ,
            TRANSACTION_YEAR      NUMBER(4),
            TRANSACTION_MONTH     NUMBER(2),
            TRANSACTION_WEEK      NUMBER(2),
            REFERENCE_NUMBER      VARCHAR(100),
            SKU_NAME              VARCHAR(200),
            CATEGORY              VARCHAR(100),
            UNIT_OF_MEASURE       VARCHAR(20),
            WAREHOUSE_NAME        VARCHAR(200),
            REGION                VARCHAR(100),
            CREATED_BY            VARCHAR(100),
            MODIFIED_DATE         TIMESTAMP_NTZ,
            LOAD_TIMESTAMP        TIMESTAMP_NTZ,
            ROW_HASH              VARCHAR(32)
        )
        CLUSTER BY (TRANSACTION_YEAR, TRANSACTION_MONTH, WAREHOUSE_ID);
    """
    sf_conn.cursor().execute(create_ddl)

    success, num_chunks, num_rows, _ = write_pandas(
        conn=sf_conn,
        df=df,
        table_name=target_table,
        overwrite=False,  # append for incremental loads
        quote_identifiers=False,
    )

    if success:
        log.info(f"Successfully loaded {num_rows:,} rows in {num_chunks} chunk(s)")
        return num_rows
    else:
        raise RuntimeError("Snowflake write_pandas returned failure status")


# ----------------------------------------------------------------
# Watermark management — track last successful load
# ----------------------------------------------------------------
def get_last_watermark(
    sf_conn: snowflake.connector.SnowflakeConnection,
    pipeline_name: str = "WMS_INVENTORY_ETL",
) -> Optional[datetime]:
    """Read the last successful run timestamp from a pipeline log table."""
    try:
        cur = sf_conn.cursor()
        cur.execute(
            """
            SELECT MAX(LOAD_TIMESTAMP)
            FROM PIPELINE_CONTROL.ETL_RUN_LOG
            WHERE PIPELINE_NAME = %s
              AND STATUS = 'SUCCESS'
            """,
            (pipeline_name,),
        )
        result = cur.fetchone()[0]
        return result  # None on first run
    except Exception:
        log.warning("Could not read watermark — defaulting to 90-day lookback")
        return None


def update_watermark(
    sf_conn: snowflake.connector.SnowflakeConnection,
    pipeline_name: str,
    rows_loaded: int,
    status: str = "SUCCESS",
    error_msg: str = None,
) -> None:
    """Write run metadata to the pipeline control table."""
    sf_conn.cursor().execute(
        """
        INSERT INTO PIPELINE_CONTROL.ETL_RUN_LOG
            (PIPELINE_NAME, LOAD_TIMESTAMP, ROWS_LOADED, STATUS, ERROR_MESSAGE)
        VALUES (%s, CURRENT_TIMESTAMP(), %s, %s, %s)
        """,
        (pipeline_name, rows_loaded, status, error_msg),
    )
    log.info(f"Watermark updated — status={status}, rows={rows_loaded:,}")


# ----------------------------------------------------------------
# Main orchestration
# ----------------------------------------------------------------
def run_pipeline(full_load: bool = False) -> None:
    """
    Orchestrate the full ETL flow:
      Extract → Transform → Load
    """
    pipeline_name = "WMS_INVENTORY_ETL"
    start_time    = datetime.now()
    rows_loaded   = 0

    log.info(f"=== Pipeline start: {pipeline_name} | mode={'FULL' if full_load else 'INCREMENTAL'} ===")

    wms_conn = sf_conn = None
    try:
        wms_conn = get_wms_connection()
        sf_conn  = get_snowflake_connection()

        watermark = None if full_load else get_last_watermark(sf_conn, pipeline_name)

        # Extract
        raw_df = extract_inventory_transactions(wms_conn, incremental_from=watermark)

        if raw_df.empty:
            log.info("No new records found — nothing to load")
            update_watermark(sf_conn, pipeline_name, 0, "SUCCESS")
            return

        # Transform
        clean_df = transform_inventory(raw_df)

        # Load
        rows_loaded = load_to_snowflake(clean_df, sf_conn)
        update_watermark(sf_conn, pipeline_name, rows_loaded, "SUCCESS")

    except Exception as exc:
        log.exception(f"Pipeline failed: {exc}")
        if sf_conn:
            update_watermark(sf_conn, pipeline_name, rows_loaded, "FAILURE", str(exc))
        raise

    finally:
        if wms_conn:
            wms_conn.close()
        if sf_conn:
            sf_conn.close()
        elapsed = (datetime.now() - start_time).seconds
        log.info(f"=== Pipeline finished in {elapsed}s | rows loaded: {rows_loaded:,} ===")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="WMS → Snowflake ETL Pipeline")
    parser.add_argument("--full-load", action="store_true",
                        help="Force a full reload instead of incremental")
    args = parser.parse_args()

    run_pipeline(full_load=args.full_load)
