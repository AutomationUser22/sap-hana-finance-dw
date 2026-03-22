"""Profit Center & Segment Analysis"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.data import has_data, get_profit_center_performance, get_quarterly_revenue_by_segment, get_cost_center_spend

st.set_page_config(page_title="Segment Analysis", page_icon="📊", layout="wide")
st.title("📊 Profit Center & Segment Analysis")
st.markdown("---")

if not has_data():
    st.info("📡 This dashboard connects to a live SAP HANA Express instance running on AWS EC2. "
        "The EC2 instance is currently stopped to avoid charges. "
        "When running, the ETL pipeline loads 10,000 balanced GL postings across 10 European entities "
        "into a star schema with 6 dimensions on HANA Column Store. "
        "See the GitHub repo for the full ETL pipeline code, schema design, and screenshots.")
    st.stop()

# ── Profit Center Performance ──
st.markdown("### Profit Center Performance")
pc = get_profit_center_performance()

if not pc.empty:
    pc["net_income"] = pc["revenue"] - pc["expenses"]
    pc["margin_pct"] = (pc["net_income"] / pc["revenue"].replace(0, 1) * 100).round(1)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=pc["profit_center_name"], y=pc["revenue"], name="Revenue", marker_color="#0070F2"))
    fig.add_trace(go.Bar(x=pc["profit_center_name"], y=pc["expenses"], name="Expenses", marker_color="#E74C3C"))
    fig.update_layout(barmode="group", height=400, plot_bgcolor="white", paper_bgcolor="white",
                      yaxis_title="Amount (€)", legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("View Detail"):
        st.dataframe(pc[["profit_center", "profit_center_name", "segment", "revenue", "expenses", "net_income", "margin_pct"]]
                     .rename(columns={"profit_center": "PC", "profit_center_name": "Name", "segment": "Segment",
                                      "revenue": "Revenue (€)", "expenses": "Expenses (€)", "net_income": "Net (€)", "margin_pct": "Margin %"}),
                     use_container_width=True, hide_index=True)

st.markdown("---")

# ── Quarterly Revenue by Segment ──
st.markdown("### Quarterly Revenue by Segment")
qtr = get_quarterly_revenue_by_segment()

if not qtr.empty:
    qtr["period"] = qtr["fiscal_year"].astype(str) + " Q" + qtr["calendar_quarter"].astype(str)
    fig = px.bar(qtr, x="period", y="revenue", color="segment", barmode="stack",
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=400, plot_bgcolor="white", paper_bgcolor="white",
                      yaxis_title="Revenue (€)", legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── Cost Center Spend ──
st.markdown("### Cost Center Spend")
cc = get_cost_center_spend()

if not cc.empty:
    fig = px.bar(cc.sort_values("total_spend", ascending=True),
                 x="total_spend", y="cost_center_name", orientation="h",
                 color="hierarchy_area", text=[f"€{s:,.0f}" for s in cc.sort_values("total_spend", ascending=True)["total_spend"]])
    fig.update_traces(textposition="outside")
    fig.update_layout(height=500, plot_bgcolor="white", paper_bgcolor="white", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
