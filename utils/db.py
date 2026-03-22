"""
SAP HANA connection and query utility.
Handles connection pooling, query execution, and config loading.
"""

import os
import yaml
import pandas as pd
import logging

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")


def load_config():
    """Load HANA connection config from YAML or Streamlit secrets."""
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "hana" in st.secrets:
            return dict(st.secrets["hana"])
    except Exception:
        pass

    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)["hana"]


def get_connection():
    """Get a HANA database connection."""
    from hdbcli import dbapi
    cfg = load_config()
    return dbapi.connect(
        address=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        databaseName=cfg.get("database", "HXE"),
    )


def execute(sql, params=None):
    """Execute a SQL statement (DDL/DML)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        conn.commit()
        return cursor.rowcount
    finally:
        cursor.close()
        conn.close()


def query(sql, params=None):
    """Execute a query and return a DataFrame."""
    conn = get_connection()
    try:
        if params:
            df = pd.read_sql(sql, conn, params=params); df.columns = df.columns.str.lower()
        else:
            df = pd.read_sql(sql, conn); df.columns = df.columns.str.lower()
        return df
    finally:
        conn.close()


def query_scalar(sql):
    """Execute a query and return a single value."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        cursor.close()
        conn.close()


def bulk_insert(table, df, schema="FINANCE_DW"):
    """Bulk insert a DataFrame into a HANA table."""
    conn = get_connection()
    cursor = conn.cursor()
    full_table = f"{schema}.{table}"

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {full_table} ({cols}) VALUES ({placeholders})"

    try:
        import numpy as np
        df = df.fillna({col: None for col in df.select_dtypes(include=[np.floating]).columns})
        df = df.where(df.notna(), None)
        data = [tuple(None if (isinstance(v, float) and v != v) else v for v in row) for row in df.itertuples(index=False, name=None)]
        cursor.executemany(sql, data)
        conn.commit()
        logger.info(f"Inserted {len(data)} rows into {full_table}")
        return len(data)
    finally:
        cursor.close()
        conn.close()
