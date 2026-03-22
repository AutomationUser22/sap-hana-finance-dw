# SAP HANA Financial Data Warehouse

Star schema financial warehouse built on SAP HANA Express Edition, with Python ETL pipeline and interactive Streamlit dashboard. Models SAP FI/CO data structures (BKPF/BSEG, GL accounts, cost centers, profit centers) across European entities.

![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)
![SAP HANA](https://img.shields.io/badge/SAP_HANA-2.0_SP08-orange.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

## Architecture

```
Python ETL Pipeline ──▶ SAP HANA Express (Column Store)
                              │
                        FINANCE_DW Schema
                              │
                    ┌─────────┼─────────┐
                    │    Star Schema    │
                    │                   │
                    │  6 Dimensions:    │
                    │  • Company Code   │
                    │  • GL Account     │
                    │  • Cost Center    │
                    │  • Profit Center  │
                    │  • Fiscal Period  │
                    │  • Document Type  │
                    │                   │
                    │  1 Fact Table:    │
                    │  • GL Postings    │
                    │    (BKPF/BSEG)   │
                    └─────────┼─────────┘
                              │
                    Streamlit Dashboard
                    • Financial Overview
                    • Segment Analysis
                    • Data Quality
```

## What This Demonstrates

| Skill | Implementation |
|-------|---------------|
| SAP HANA | Column store tables, HANA SQL, GENERATED IDENTITY |
| SAP Data Model | GL accounts (CoA), company codes, cost/profit centers, document types |
| Financial Modeling | Balanced debit/credit postings, multi-currency, fiscal periods |
| Star Schema | 6 dimensions + 1 fact table with surrogate keys |
| ETL Pipeline | Python → HANA bulk insert with validation |
| Reconciliation | Debit/credit balance verification per period |
| BI Dashboard | Streamlit + Plotly on live HANA data |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Update config.yaml with your HANA connection details

# Run ETL pipeline
python etl_pipeline.py

# Launch dashboard
streamlit run app.py
```

## HANA Setup

Requires SAP HANA Express Edition running on Docker (EC2 or Linux):

```bash
sudo docker pull saplabs/hanaexpress:latest
sudo docker run -d -p 39013:39013 -p 39017:39017 ...
```

See `docs/hana_setup.md` for full setup instructions.

## Dashboard Pages

| Page | What It Shows |
|------|---------------|
| **Financial Overview** | Revenue/expense trends, entity breakdown, KPIs |
| **Segment Analysis** | Profit center P&L, quarterly revenue by segment, cost center spend |
| **Data Quality** | Debit/credit balance checks, document type distribution, table stats |

## License

MIT
