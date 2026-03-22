"""
ETL Pipeline — Load financial data into SAP HANA.

Generates realistic SAP-style financial transactions and loads them
into the FINANCE_DW star schema on HANA Express.
"""

import random
import logging
import time
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd

from utils.db import get_connection, bulk_insert, execute, query_scalar

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCHEMA = "FINANCE_DW"


# ══════════════════════════════════════════════════════════════
# DIMENSION DATA GENERATORS
# ══════════════════════════════════════════════════════════════

def load_company_codes():
    """Load SAP company code dimension — European entities."""
    data = [
        ("1000", "Amazon EU S.a.r.l.", "Luxembourg", "EUR", "EMEA"),
        ("1100", "Amazon Europe Core", "Luxembourg", "EUR", "EMEA"),
        ("1200", "Amazon Services Europe", "Luxembourg", "EUR", "EMEA"),
        ("2000", "Amazon UK Services Ltd", "United Kingdom", "GBP", "EMEA"),
        ("2100", "Amazon Development Centre", "Ireland", "EUR", "EMEA"),
        ("3000", "Amazon Deutschland GmbH", "Germany", "EUR", "EMEA"),
        ("3100", "Amazon Logistik GmbH", "Germany", "EUR", "EMEA"),
        ("4000", "Amazon France SAS", "France", "EUR", "EMEA"),
        ("5000", "Amazon Italia Logistica", "Italy", "EUR", "EMEA"),
        ("6000", "Amazon Spain Services", "Spain", "EUR", "EMEA"),
    ]
    df = pd.DataFrame(data, columns=["company_code", "company_name", "country", "currency", "region"])
    return df


def load_gl_accounts():
    """Load SAP GL account dimension — Chart of Accounts."""
    data = [
        ("100000", "Cash and Bank", "Asset", "Current Assets", "Cash"),
        ("110000", "Accounts Receivable", "Asset", "Current Assets", "Receivables"),
        ("120000", "Inventory", "Asset", "Current Assets", "Inventory"),
        ("150000", "Fixed Assets", "Asset", "Non-Current Assets", "PPE"),
        ("200000", "Accounts Payable", "Liability", "Current Liabilities", "Payables"),
        ("210000", "Accrued Expenses", "Liability", "Current Liabilities", "Accruals"),
        ("220000", "Tax Payable", "Liability", "Current Liabilities", "Tax"),
        ("300000", "Retained Earnings", "Equity", "Equity", "Retained Earnings"),
        ("400000", "Product Revenue", "Revenue", "Revenue", "Revenue"),
        ("410000", "Service Revenue", "Revenue", "Revenue", "Revenue"),
        ("420000", "Subscription Revenue", "Revenue", "Revenue", "Revenue"),
        ("430000", "AWS Revenue", "Revenue", "Revenue", "Revenue"),
        ("440000", "Advertising Revenue", "Revenue", "Revenue", "Revenue"),
        ("500000", "Cost of Goods Sold", "Expense", "COGS", "COGS"),
        ("510000", "Fulfillment Costs", "Expense", "COGS", "COGS"),
        ("600000", "Salaries & Wages", "Expense", "Operating Expenses", "Personnel"),
        ("610000", "Benefits & Insurance", "Expense", "Operating Expenses", "Personnel"),
        ("620000", "Technology & Content", "Expense", "Operating Expenses", "Technology"),
        ("630000", "Marketing Expense", "Expense", "Operating Expenses", "Marketing"),
        ("640000", "General & Admin", "Expense", "Operating Expenses", "G&A"),
        ("650000", "Depreciation", "Expense", "Operating Expenses", "Depreciation"),
        ("700000", "Interest Income", "Revenue", "Other Income", "Interest"),
        ("710000", "Interest Expense", "Expense", "Other Expenses", "Interest"),
        ("720000", "Foreign Exchange Gain/Loss", "Expense", "Other Expenses", "FX"),
    ]
    df = pd.DataFrame(data, columns=["gl_account", "gl_account_name", "account_type", "account_group", "fs_line_item"])
    return df


def load_cost_centers():
    """Load cost center dimension."""
    data = [
        ("CC1000", "Executive Office", "Executive", "M. Johnson", "Corporate"),
        ("CC1100", "Finance & Accounting", "Finance", "S. Williams", "Corporate"),
        ("CC1200", "Human Resources", "HR", "A. Brown", "Corporate"),
        ("CC2000", "Software Engineering", "Engineering", "T. Chen", "Technology"),
        ("CC2100", "Data Engineering", "Engineering", "R. Kumar", "Technology"),
        ("CC2200", "Cloud Operations", "Engineering", "K. Mueller", "Technology"),
        ("CC3000", "Fulfillment Center Ops", "Operations", "J. Garcia", "Operations"),
        ("CC3100", "Transportation & Logistics", "Operations", "L. Martin", "Operations"),
        ("CC3200", "Customer Service", "Operations", "P. Anderson", "Operations"),
        ("CC4000", "Product Marketing", "Marketing", "D. Taylor", "Marketing"),
        ("CC4100", "Performance Marketing", "Marketing", "E. Wilson", "Marketing"),
        ("CC5000", "EU Sales", "Sales", "F. Laurent", "Sales"),
        ("CC5100", "UK Sales", "Sales", "G. Smith", "Sales"),
    ]
    df = pd.DataFrame(data, columns=["cost_center", "cost_center_name", "department", "responsible_person", "hierarchy_area"])
    return df


def load_profit_centers():
    """Load profit center dimension."""
    data = [
        ("PC100", "EU Marketplace", "Retail", "E-Commerce"),
        ("PC200", "UK Marketplace", "Retail", "E-Commerce"),
        ("PC300", "AWS EMEA", "Cloud", "Technology"),
        ("PC400", "Advertising EMEA", "Advertising", "Digital"),
        ("PC500", "Prime EMEA", "Subscription", "Membership"),
        ("PC600", "Logistics Services", "Operations", "Fulfillment"),
        ("PC700", "Corporate Services", "Corporate", "Support"),
    ]
    df = pd.DataFrame(data, columns=["profit_center", "profit_center_name", "segment", "business_area"])
    return df


def load_document_types():
    """Load SAP document type dimension."""
    data = [
        ("SA", "GL Account Document", "GL"),
        ("RE", "Invoice Receipt", "AP"),
        ("KR", "Vendor Credit Memo", "AP"),
        ("DR", "Customer Invoice", "AR"),
        ("DG", "Customer Credit Memo", "AR"),
        ("AA", "Asset Posting", "Asset"),
        ("AB", "Accrual Document", "Accrual"),
        ("SB", "GL Reversal", "GL"),
    ]
    df = pd.DataFrame(data, columns=["doc_type", "doc_type_name", "category"])
    return df


def load_fiscal_periods():
    """Generate fiscal period dimension for 2022-2025."""
    periods = []
    for year in range(2022, 2026):
        for month in range(1, 13):
            start = date(year, month, 1)
            if month == 12:
                end = date(year, 12, 31)
            else:
                end = date(year, month + 1, 1) - timedelta(days=1)

            periods.append({
                "fiscal_year": str(year),
                "fiscal_period": f"{month:03d}",
                "posting_date": start,
                "calendar_year": year,
                "calendar_month": month,
                "calendar_quarter": (month - 1) // 3 + 1,
                "month_name": start.strftime("%B"),
                "period_start_date": start,
                "period_end_date": end,
            })
    return pd.DataFrame(periods)


# ══════════════════════════════════════════════════════════════
# FACT DATA GENERATOR
# ══════════════════════════════════════════════════════════════

def generate_gl_postings(num_documents=5000, seed=42):
    """Generate realistic GL posting transactions."""
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)

    # Load dimension keys (we'll need to query them after dims are loaded)
    conn = get_connection()
    companies = pd.read_sql(f"SELECT company_code_key, company_code, currency FROM {SCHEMA}.DIM_COMPANY_CODE", conn)
    companies.columns = companies.columns.str.lower()
    companies.columns = companies.columns.str.lower()
    gl_accounts = pd.read_sql(f"SELECT gl_account_key, gl_account, account_type, fs_line_item FROM {SCHEMA}.DIM_GL_ACCOUNT", conn)
    gl_accounts.columns = gl_accounts.columns.str.lower()
    cost_centers = pd.read_sql(f"SELECT cost_center_key, cost_center FROM {SCHEMA}.DIM_COST_CENTER", conn)
    cost_centers.columns = cost_centers.columns.str.lower()
    cost_centers.columns = cost_centers.columns.str.lower()
    profit_centers = pd.read_sql(f"SELECT profit_center_key, profit_center FROM {SCHEMA}.DIM_PROFIT_CENTER", conn)
    profit_centers.columns = profit_centers.columns.str.lower()
    profit_centers.columns = profit_centers.columns.str.lower()
    fiscal_periods = pd.read_sql(f"SELECT fiscal_period_key, fiscal_year, fiscal_period, period_start_date, period_end_date FROM {SCHEMA}.DIM_FISCAL_PERIOD", conn)
    fiscal_periods.columns = fiscal_periods.columns.str.lower()
    fiscal_periods.columns = fiscal_periods.columns.str.lower()
    doc_types = pd.read_sql(f"SELECT doc_type_key, doc_type, category FROM {SCHEMA}.DIM_DOCUMENT_TYPE", conn)
    doc_types.columns = doc_types.columns.str.lower()
    doc_types.columns = doc_types.columns.str.lower()
    conn.close()

    revenue_accounts = gl_accounts[gl_accounts["account_type"] == "Revenue"]["gl_account_key"].tolist()
    expense_accounts = gl_accounts[gl_accounts["account_type"] == "Expense"]["gl_account_key"].tolist()
    asset_accounts = gl_accounts[gl_accounts["account_type"] == "Asset"]["gl_account_key"].tolist()
    liability_accounts = gl_accounts[gl_accounts["account_type"] == "Liability"]["gl_account_key"].tolist()

    postings = []
    doc_counter = 0

    for _ in range(num_documents):
        doc_counter += 1
        doc_number = f"{doc_counter:010d}"

        # Pick random company, period, doc type
        company = companies.iloc[rng.randint(0, len(companies) - 1)]
        period = fiscal_periods.iloc[rng.randint(0, len(fiscal_periods) - 1)]
        doc_type = doc_types.iloc[rng.randint(0, len(doc_types) - 1)]
        cost_center = cost_centers.iloc[rng.randint(0, len(cost_centers) - 1)]
        profit_center = profit_centers.iloc[rng.randint(0, len(profit_centers) - 1)]

        # Generate posting date within the period
        start_d = pd.to_datetime(period["period_start_date"]).date() if not isinstance(period["period_start_date"], date) else period["period_start_date"]
        end_d = pd.to_datetime(period["period_end_date"]).date() if not isinstance(period["period_end_date"], date) else period["period_end_date"]
        days_range = (end_d - start_d).days
        posting_date = start_d + timedelta(days=rng.randint(0, max(days_range, 0)))

        # Amount (log-normal distribution for realistic spread)
        base_amount = round(float(np_rng.lognormal(8, 2)), 2)
        base_amount = min(base_amount, 5000000)  # Cap at 5M

        # FX rate for non-EUR companies
        fx_rate = 1.0
        if company["currency"] == "GBP":
            fx_rate = rng.uniform(1.12, 1.18)
        usd_rate = rng.uniform(1.05, 1.15)

        # Generate balanced debit/credit lines (every document must balance)
        category = doc_type["category"]

        if category == "AR":
            debit_account = gl_accounts[gl_accounts["fs_line_item"] == "Receivables"]["gl_account_key"].iloc[0]
            credit_account = rng.choice(revenue_accounts)
        elif category == "AP":
            debit_account = rng.choice(expense_accounts)
            credit_account = gl_accounts[gl_accounts["fs_line_item"] == "Payables"]["gl_account_key"].iloc[0]
        elif category == "Asset":
            debit_account = gl_accounts[gl_accounts["fs_line_item"] == "PPE"]["gl_account_key"].iloc[0]
            credit_account = gl_accounts[gl_accounts["fs_line_item"] == "Cash"]["gl_account_key"].iloc[0]
        else:
            debit_account = rng.choice(expense_accounts + asset_accounts)
            credit_account = rng.choice(revenue_accounts + liability_accounts)

        amount_local = round(base_amount * fx_rate, 2)
        amount_usd = round(base_amount * usd_rate, 2)

        tax_code = rng.choice(["V0", "V1", "V7", "A0", "A1", None, None])
        reference = f"REF{rng.randint(100000, 999999)}" if rng.random() > 0.3 else None
        header_text = rng.choice(["Monthly posting", "Invoice payment", "Accrual entry",
                                   "Revenue recognition", "Cost allocation", "Intercompany",
                                   "FX revaluation", "Depreciation run", None])
        created_by = rng.choice(["BATCH_JOB", "ETL_USER", "FI_USER01", "FI_USER02", "SYSTEM"])

        # Debit line
        postings.append({
            "document_number": doc_number,
            "line_item": 1,
            "company_code_key": int(company["company_code_key"]),
            "gl_account_key": int(debit_account),
            "cost_center_key": int(cost_center["cost_center_key"]),
            "profit_center_key": int(profit_center["profit_center_key"]),
            "fiscal_period_key": int(period["fiscal_period_key"]),
            "doc_type_key": int(doc_type["doc_type_key"]),
            "posting_date": posting_date,
            "amount_local": amount_local,
            "amount_usd": amount_usd,
            "debit_credit": "D",
            "tax_code": tax_code,
            "reference": reference,
            "document_header_text": header_text,
            "created_by": created_by,
        })

        # Credit line (balancing entry)
        postings.append({
            "document_number": doc_number,
            "line_item": 2,
            "company_code_key": int(company["company_code_key"]),
            "gl_account_key": int(credit_account),
            "cost_center_key": int(cost_center["cost_center_key"]),
            "profit_center_key": int(profit_center["profit_center_key"]),
            "fiscal_period_key": int(period["fiscal_period_key"]),
            "doc_type_key": int(doc_type["doc_type_key"]),
            "posting_date": posting_date,
            "amount_local": amount_local,
            "amount_usd": amount_usd,
            "debit_credit": "C",
            "tax_code": tax_code,
            "reference": reference,
            "document_header_text": header_text,
            "created_by": created_by,
        })

    return pd.DataFrame(postings)


# ══════════════════════════════════════════════════════════════
# ETL PIPELINE
# ══════════════════════════════════════════════════════════════

def run_pipeline():
    """Run the full ETL pipeline."""
    start = time.time()

    print("=" * 60)
    print("SAP HANA FINANCIAL DATA WAREHOUSE — ETL PIPELINE")
    print("=" * 60)

    # Check connection
    print("\nTesting HANA connection...")
    version = query_scalar("SELECT VALUE FROM SYS.M_SYSTEM_OVERVIEW WHERE NAME = 'Version'")
    print(f"Connected to HANA: {version}")

    # Clear existing data
    print("\nClearing existing data...")
    for table in ["FACT_GL_POSTING", "DIM_FISCAL_PERIOD", "DIM_DOCUMENT_TYPE",
                   "DIM_PROFIT_CENTER", "DIM_COST_CENTER", "DIM_GL_ACCOUNT", "DIM_COMPANY_CODE"]:
        execute(f"DELETE FROM {SCHEMA}.{table}")
    print("  Tables cleared")

    # Load dimensions
    print("\nLoading dimensions...")

    dims = {
        "DIM_COMPANY_CODE": load_company_codes(),
        "DIM_GL_ACCOUNT": load_gl_accounts(),
        "DIM_COST_CENTER": load_cost_centers(),
        "DIM_PROFIT_CENTER": load_profit_centers(),
        "DIM_FISCAL_PERIOD": load_fiscal_periods(),
        "DIM_DOCUMENT_TYPE": load_document_types(),
    }

    for table, df in dims.items():
        count = bulk_insert(table, df)
        print(f"  {table}: {count} rows")

    # Generate and load fact data
    print("\nGenerating GL postings...")
    postings = generate_gl_postings(num_documents=5000)
    print(f"  Generated {len(postings)} posting lines from 5,000 documents")

    print("\nLoading fact table...")
    count = bulk_insert("FACT_GL_POSTING", postings)
    print(f"  FACT_GL_POSTING: {count} rows")

    # Verify
    print("\nVerification:")
    for table in ["DIM_COMPANY_CODE", "DIM_GL_ACCOUNT", "DIM_COST_CENTER",
                   "DIM_PROFIT_CENTER", "DIM_FISCAL_PERIOD", "DIM_DOCUMENT_TYPE", "FACT_GL_POSTING"]:
        cnt = query_scalar(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
        print(f"  {table}: {cnt} rows")

    # Balance check
    debit_sum = query_scalar(f"SELECT ROUND(SUM(amount_local), 2) FROM {SCHEMA}.FACT_GL_POSTING WHERE debit_credit = 'D'")
    credit_sum = query_scalar(f"SELECT ROUND(SUM(amount_local), 2) FROM {SCHEMA}.FACT_GL_POSTING WHERE debit_credit = 'C'")
    print(f"\n  Debit total:  {debit_sum:,.2f}")
    print(f"  Credit total: {credit_sum:,.2f}")
    print(f"  Balanced: {'YES' if abs(debit_sum - credit_sum) < 0.01 else 'NO'}")

    elapsed = round(time.time() - start, 2)
    print(f"\nPipeline complete in {elapsed}s")
    print("=" * 60)


if __name__ == "__main__":
    run_pipeline()
