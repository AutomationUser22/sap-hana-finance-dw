"""Data access layer for the HANA financial dashboard."""

from utils.db import query

SCHEMA = "FINANCE_DW"


def has_data():
    try:
        df = query(f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.FACT_GL_POSTING")
        return df.iloc[0]["cnt"] > 0
    except Exception:
        return False


def get_revenue_by_period():
    return query(f"""
        SELECT fp.fiscal_year, fp.calendar_month, fp.month_name, fp.calendar_quarter,
               ROUND(SUM(CASE WHEN f.debit_credit = 'C' THEN f.amount_local ELSE 0 END), 2) AS revenue,
               ROUND(SUM(CASE WHEN f.debit_credit = 'D' THEN f.amount_local ELSE 0 END), 2) AS expenses,
               ROUND(SUM(CASE WHEN f.debit_credit = 'C' THEN f.amount_local ELSE -f.amount_local END), 2) AS net_amount
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_GL_ACCOUNT gl ON f.gl_account_key = gl.gl_account_key
        JOIN {SCHEMA}.DIM_FISCAL_PERIOD fp ON f.fiscal_period_key = fp.fiscal_period_key
        WHERE gl.account_type IN ('Revenue', 'Expense')
        GROUP BY fp.fiscal_year, fp.calendar_month, fp.month_name, fp.calendar_quarter
        ORDER BY fp.fiscal_year, fp.calendar_month
    """)


def get_revenue_by_company():
    return query(f"""
        SELECT cc.company_code, cc.company_name, cc.country, cc.currency,
               ROUND(SUM(CASE WHEN gl.account_type = 'Revenue' AND f.debit_credit = 'C' THEN f.amount_local ELSE 0 END), 2) AS revenue,
               ROUND(SUM(CASE WHEN gl.account_type = 'Expense' AND f.debit_credit = 'D' THEN f.amount_local ELSE 0 END), 2) AS expenses,
               COUNT(DISTINCT f.document_number) AS documents
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_COMPANY_CODE cc ON f.company_code_key = cc.company_code_key
        JOIN {SCHEMA}.DIM_GL_ACCOUNT gl ON f.gl_account_key = gl.gl_account_key
        GROUP BY cc.company_code, cc.company_name, cc.country, cc.currency
        ORDER BY revenue DESC
    """)


def get_expense_by_category():
    return query(f"""
        SELECT gl.account_group, gl.fs_line_item,
               ROUND(SUM(f.amount_local), 2) AS amount,
               COUNT(DISTINCT f.document_number) AS documents
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_GL_ACCOUNT gl ON f.gl_account_key = gl.gl_account_key
        WHERE gl.account_type = 'Expense' AND f.debit_credit = 'D'
        GROUP BY gl.account_group, gl.fs_line_item
        ORDER BY amount DESC
    """)


def get_cost_center_spend():
    return query(f"""
        SELECT cc.cost_center, cc.cost_center_name, cc.department, cc.hierarchy_area,
               ROUND(SUM(f.amount_local), 2) AS total_spend,
               COUNT(DISTINCT f.document_number) AS documents
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_COST_CENTER cc ON f.cost_center_key = cc.cost_center_key
        WHERE f.debit_credit = 'D'
        GROUP BY cc.cost_center, cc.cost_center_name, cc.department, cc.hierarchy_area
        ORDER BY total_spend DESC
    """)


def get_profit_center_performance():
    return query(f"""
        SELECT pc.profit_center, pc.profit_center_name, pc.segment, pc.business_area,
               ROUND(SUM(CASE WHEN gl.account_type = 'Revenue' AND f.debit_credit = 'C' THEN f.amount_local ELSE 0 END), 2) AS revenue,
               ROUND(SUM(CASE WHEN gl.account_type = 'Expense' AND f.debit_credit = 'D' THEN f.amount_local ELSE 0 END), 2) AS expenses
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_PROFIT_CENTER pc ON f.profit_center_key = pc.profit_center_key
        JOIN {SCHEMA}.DIM_GL_ACCOUNT gl ON f.gl_account_key = gl.gl_account_key
        GROUP BY pc.profit_center, pc.profit_center_name, pc.segment, pc.business_area
        ORDER BY revenue DESC
    """)


def get_document_type_summary():
    return query(f"""
        SELECT dt.doc_type, dt.doc_type_name, dt.category,
               COUNT(*) AS line_items,
               COUNT(DISTINCT f.document_number) AS documents,
               ROUND(SUM(f.amount_local), 2) AS total_amount
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_DOCUMENT_TYPE dt ON f.doc_type_key = dt.doc_type_key
        GROUP BY dt.doc_type, dt.doc_type_name, dt.category
        ORDER BY documents DESC
    """)


def get_monthly_balance_check():
    return query(f"""
        SELECT fp.fiscal_year, fp.calendar_month, fp.month_name,
               ROUND(SUM(CASE WHEN f.debit_credit = 'D' THEN f.amount_local ELSE 0 END), 2) AS total_debits,
               ROUND(SUM(CASE WHEN f.debit_credit = 'C' THEN f.amount_local ELSE 0 END), 2) AS total_credits,
               ROUND(SUM(CASE WHEN f.debit_credit = 'D' THEN f.amount_local ELSE 0 END) -
                     SUM(CASE WHEN f.debit_credit = 'C' THEN f.amount_local ELSE 0 END), 2) AS variance
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_FISCAL_PERIOD fp ON f.fiscal_period_key = fp.fiscal_period_key
        GROUP BY fp.fiscal_year, fp.calendar_month, fp.month_name
        ORDER BY fp.fiscal_year, fp.calendar_month
    """)


def get_table_stats():
    tables = ["DIM_COMPANY_CODE", "DIM_GL_ACCOUNT", "DIM_COST_CENTER",
              "DIM_PROFIT_CENTER", "DIM_FISCAL_PERIOD", "DIM_DOCUMENT_TYPE", "FACT_GL_POSTING"]
    stats = []
    for t in tables:
        df = query(f"SELECT COUNT(*) AS cnt FROM {SCHEMA}.{t}")
        stats.append({"table": t, "row_count": int(df.iloc[0]["cnt"])})
    return stats


def get_quarterly_revenue_by_segment():
    return query(f"""
        SELECT fp.fiscal_year, fp.calendar_quarter, pc.segment,
               ROUND(SUM(CASE WHEN gl.account_type = 'Revenue' AND f.debit_credit = 'C' THEN f.amount_local ELSE 0 END), 2) AS revenue
        FROM {SCHEMA}.FACT_GL_POSTING f
        JOIN {SCHEMA}.DIM_PROFIT_CENTER pc ON f.profit_center_key = pc.profit_center_key
        JOIN {SCHEMA}.DIM_GL_ACCOUNT gl ON f.gl_account_key = gl.gl_account_key
        JOIN {SCHEMA}.DIM_FISCAL_PERIOD fp ON f.fiscal_period_key = fp.fiscal_period_key
        GROUP BY fp.fiscal_year, fp.calendar_quarter, pc.segment
        ORDER BY fp.fiscal_year, fp.calendar_quarter
    """)
