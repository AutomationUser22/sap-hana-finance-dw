"""SAP HANA Financial Data Warehouse — Dashboard"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.data import (has_data, get_revenue_by_period, get_revenue_by_company,
                        get_expense_by_category, get_table_stats)

st.set_page_config(page_title="SAP HANA Finance DW", page_icon="💰", layout="wide")

st.title("💰 SAP HANA Financial Data Warehouse")
st.caption("Star schema warehouse on SAP HANA Express — European entity financial data")
st.markdown("---")

if not has_data():
    st.info("📡 This dashboard connects to a live SAP HANA Express instance running on AWS EC2. "
        "The EC2 instance is currently stopped to avoid charges. "
        "When running, the ETL pipeline loads 10,000 balanced GL postings across 10 European entities "
        "into a star schema with 6 dimensions on HANA Column Store. "
        "See the GitHub repo for the full ETL pipeline code, schema design, and screenshots.")
    st.stop()

# ── KPIs ──
rev_data = get_revenue_by_period()
if not rev_data.empty:
    total_rev = rev_data["revenue"].sum()
    total_exp = rev_data["expenses"].sum()
    net = total_rev - total_exp
    margin = round(net / total_rev * 100, 1) if total_rev > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Revenue", f"€{total_rev:,.0f}")
    with col2:
        st.metric("Total Expenses", f"€{total_exp:,.0f}")
    with col3:
        st.metric("Net Income", f"€{net:,.0f}")
    with col4:
        st.metric("Margin", f"{margin}%")

st.markdown("---")

# ── Revenue Trend ──
st.markdown("### Monthly Revenue & Expenses")

if not rev_data.empty:
    rev_data["period"] = rev_data["fiscal_year"].astype(str) + "-" + rev_data["calendar_month"].astype(str).str.zfill(2)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=rev_data["period"], y=rev_data["revenue"], mode="lines+markers",
                             name="Revenue", line=dict(color="#0070F2", width=2)))
    fig.add_trace(go.Scatter(x=rev_data["period"], y=rev_data["expenses"], mode="lines+markers",
                             name="Expenses", line=dict(color="#E74C3C", width=2)))
    fig.update_layout(height=400, plot_bgcolor="white", paper_bgcolor="white",
                      yaxis_title="Amount (€)", legend=dict(orientation="h", y=1.1))
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#F0F0F0")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Revenue by Company ──
col1, col2 = st.columns(2)

with col1:
    st.markdown("### Revenue by Entity")
    comp = get_revenue_by_company()
    if not comp.empty:
        comp_rev = comp[comp["revenue"] > 0].sort_values("revenue", ascending=True)
        fig = px.bar(comp_rev, x="revenue", y="company_name", orientation="h",
                     color="country", text=[f"€{r:,.0f}" for r in comp_rev["revenue"]])
        fig.update_traces(textposition="outside")
        fig.update_layout(height=400, plot_bgcolor="white", paper_bgcolor="white",
                          showlegend=True, yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("### Expense Breakdown")
    exp = get_expense_by_category()
    if not exp.empty:
        fig = px.pie(exp, values="amount", names="fs_line_item", hole=0.4,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_traces(textposition="outside", textinfo="label+percent")
        fig.update_layout(height=400, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Table Stats ──
st.markdown("### Warehouse Statistics")
stats = get_table_stats()
cols = st.columns(len(stats))
for i, s in enumerate(stats):
    with cols[i]:
        label = s["table"].replace("DIM_", "").replace("FACT_", "").replace("_", " ").title()
        st.metric(label, f"{s['row_count']:,}")

st.markdown("---")
st.caption("Powered by SAP HANA Express Edition 2.0 | Star Schema: 6 Dimensions + 1 Fact Table | HANA Column Store")
