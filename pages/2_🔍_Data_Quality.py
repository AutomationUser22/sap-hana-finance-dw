"""Data Quality & Reconciliation Checks"""

import streamlit as st
import plotly.graph_objects as go

from utils.data import has_data, get_monthly_balance_check, get_document_type_summary, get_table_stats

st.set_page_config(page_title="Data Quality", page_icon="🔍", layout="wide")
st.title("🔍 Data Quality & Reconciliation")
st.markdown("---")

if not has_data():
    st.warning("No data. Run `python etl_pipeline.py` first.")
    st.stop()

# ── Debit/Credit Balance Check ──
st.markdown("### Monthly Debit/Credit Balance Check")
st.caption("Every accounting document must balance: Total Debits = Total Credits. Variance should be zero.")

balance = get_monthly_balance_check()
if not balance.empty:
    balance["period"] = balance["fiscal_year"].astype(str) + "-" + balance["calendar_month"].astype(str).str.zfill(2)
    all_balanced = (balance["variance"].abs() < 0.01).all()

    if all_balanced:
        st.success("All periods are balanced — Total Debits = Total Credits for every month.")
    else:
        unbalanced = balance[balance["variance"].abs() >= 0.01]
        st.error(f"{len(unbalanced)} periods have balance variances!")

    fig = go.Figure()
    fig.add_trace(go.Bar(x=balance["period"], y=balance["total_debits"], name="Debits", marker_color="#0070F2"))
    fig.add_trace(go.Bar(x=balance["period"], y=balance["total_credits"], name="Credits", marker_color="#00C9A7"))
    fig.update_layout(barmode="group", height=400, plot_bgcolor="white", paper_bgcolor="white",
                      yaxis_title="Amount (€)", legend=dict(orientation="h", y=1.1))
    fig.update_xaxes(showgrid=False)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("View Monthly Balance Detail"):
        st.dataframe(balance[["period", "total_debits", "total_credits", "variance"]]
                     .rename(columns={"period": "Period", "total_debits": "Debits (€)", "total_credits": "Credits (€)", "variance": "Variance (€)"}),
                     use_container_width=True, hide_index=True)

st.markdown("---")

# ── Document Type Distribution ──
st.markdown("### Document Type Distribution")
doc_types = get_document_type_summary()

if not doc_types.empty:
    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure(go.Bar(x=doc_types["doc_type_name"], y=doc_types["documents"],
                               marker_color="#0070F2",
                               text=doc_types["documents"], textposition="outside"))
        fig.update_layout(height=350, plot_bgcolor="white", paper_bgcolor="white",
                          yaxis_title="Document Count")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(doc_types[["doc_type", "doc_type_name", "category", "documents", "line_items", "total_amount"]]
                     .rename(columns={"doc_type": "Type", "doc_type_name": "Name", "category": "Category",
                                      "documents": "Docs", "line_items": "Lines", "total_amount": "Amount (€)"}),
                     use_container_width=True, hide_index=True)

st.markdown("---")

# ── Warehouse Stats ──
st.markdown("### Warehouse Table Statistics")
stats = get_table_stats()

cols = st.columns(len(stats))
for i, s in enumerate(stats):
    with cols[i]:
        ttype = "Dim" if s["table"].startswith("DIM") else "Fact"
        label = s["table"].replace("DIM_", "").replace("FACT_", "").replace("_", " ").title()
        st.metric(label, f"{s['row_count']:,}")
        st.caption(ttype)

st.markdown("---")

st.markdown("""
### Architecture

```
SAP HANA Express Edition 2.0 SP08
    │
    ├── Schema: FINANCE_DW
    │
    ├── Dimensions (COLUMN STORE):
    │   ├── DIM_COMPANY_CODE    — 10 European entities
    │   ├── DIM_GL_ACCOUNT      — 24 GL accounts (SAP CoA)
    │   ├── DIM_COST_CENTER     — 13 cost centers
    │   ├── DIM_PROFIT_CENTER   — 7 profit centers
    │   ├── DIM_FISCAL_PERIOD   — 48 months (2022-2025)
    │   └── DIM_DOCUMENT_TYPE   — 8 SAP document types
    │
    └── Fact (COLUMN STORE):
        └── FACT_GL_POSTING     — 10,000 balanced GL line items
            Modeled after SAP BKPF/BSEG tables
```
""")
