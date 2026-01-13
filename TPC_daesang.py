import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
import pandas as pd
import altair as alt

st.set_page_config(page_title="TPC-H Analytics Dashboard", page_icon="❄️", layout="wide")

session = get_active_session()

st.title("❄️ TPC-H Sales Analytics Dashboard")
st.markdown("Snowflake Sample Data를 활용한 매출 분석 대시보드")

st.sidebar.header("필터 설정")

@st.cache_data
def get_regions():
    return session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION").select("R_NAME").to_pandas()["R_NAME"].tolist()

@st.cache_data
def get_date_range():
    result = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS").select(
        F.min("O_ORDERDATE").alias("MIN_DATE"),
        F.max("O_ORDERDATE").alias("MAX_DATE")
    ).to_pandas()
    return result["MIN_DATE"][0], result["MAX_DATE"][0]

regions = get_regions()
selected_regions = st.sidebar.multiselect("지역 선택", regions, default=regions)

min_date, max_date = get_date_range()
date_range = st.sidebar.date_input(
    "기간 선택",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

@st.cache_data
def load_filtered_data(_selected_regions, start_date, end_date):
    orders_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS")
    customer_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
    nation_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION")
    region_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION")
    lineitem_df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")
    
    base_df = (
        orders_df
        .filter((F.col("O_ORDERDATE") >= start_date) & (F.col("O_ORDERDATE") <= end_date))
        .join(customer_df, orders_df["O_CUSTKEY"] == customer_df["C_CUSTKEY"])
        .join(nation_df, customer_df["C_NATIONKEY"] == nation_df["N_NATIONKEY"])
        .join(region_df, nation_df["N_REGIONKEY"] == region_df["R_REGIONKEY"])
        .filter(F.col("R_NAME").isin(_selected_regions))
    )
    
    total_revenue = base_df.agg(F.sum("O_TOTALPRICE").alias("TOTAL")).to_pandas()["TOTAL"][0]
    total_orders = base_df.count()
    avg_order_value = base_df.agg(F.avg("O_TOTALPRICE").alias("AVG")).to_pandas()["AVG"][0]
    
    monthly_revenue = (
        base_df
        .with_column("YEAR_MONTH", F.date_trunc("MONTH", F.col("O_ORDERDATE")))
        .group_by("YEAR_MONTH")
        .agg(F.sum("O_TOTALPRICE").alias("REVENUE"))
        .order_by("YEAR_MONTH")
        .to_pandas()
    )
    
    region_revenue = (
        base_df
        .group_by("R_NAME")
        .agg(
            F.sum("O_TOTALPRICE").alias("REVENUE"),
            F.count("O_ORDERKEY").alias("ORDERS")
        )
        .to_pandas()
    )
    
    segment_analysis = (
        base_df
        .group_by("C_MKTSEGMENT")
        .agg(
            F.sum("O_TOTALPRICE").alias("REVENUE"),
            F.count("O_ORDERKEY").alias("ORDERS"),
            F.avg("O_TOTALPRICE").alias("AVG_ORDER")
        )
        .to_pandas()
    )
    
    priority_analysis = (
        base_df
        .group_by("O_ORDERPRIORITY")
        .agg(
            F.count("O_ORDERKEY").alias("ORDERS"),
            F.sum("O_TOTALPRICE").alias("REVENUE")
        )
        .order_by("O_ORDERPRIORITY")
        .to_pandas()
    )
    
    nation_revenue = (
        base_df
        .group_by("N_NAME", "R_NAME")
        .agg(F.sum("O_TOTALPRICE").alias("REVENUE"))
        .order_by(F.col("REVENUE").desc())
        .limit(10)
        .to_pandas()
    )
    
    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "avg_order_value": avg_order_value,
        "monthly_revenue": monthly_revenue,
        "region_revenue": region_revenue,
        "segment_analysis": segment_analysis,
        "priority_analysis": priority_analysis,
        "nation_revenue": nation_revenue
    }

if len(date_range) == 2 and selected_regions:
    data = load_filtered_data(tuple(selected_regions), str(date_range[0]), str(date_range[1]))
    
    st.header("📊 핵심 지표")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("총 매출", f"${data['total_revenue']:,.0f}")
    with col2:
        st.metric("총 주문 수", f"{data['total_orders']:,}")
    with col3:
        st.metric("평균 주문액", f"${data['avg_order_value']:,.2f}")
    
    st.divider()
    
    st.header("📈 매출 트렌드")
    monthly_chart = alt.Chart(data["monthly_revenue"]).mark_area(
        line={"color": "#29B5E8"},
        color=alt.Gradient(
            gradient="linear",
            stops=[alt.GradientStop(color="white", offset=0), alt.GradientStop(color="#29B5E8", offset=1)],
            x1=1, x2=1, y1=1, y2=0
        )
    ).encode(
        x=alt.X("YEAR_MONTH:T", title="월"),
        y=alt.Y("REVENUE:Q", title="매출 ($)", axis=alt.Axis(format="~s")),
        tooltip=[alt.Tooltip("YEAR_MONTH:T", title="월"), alt.Tooltip("REVENUE:Q", title="매출", format="$,.0f")]
    ).properties(height=350)
    st.altair_chart(monthly_chart, use_container_width=True)
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("🌍 지역별 매출")
        region_chart = alt.Chart(data["region_revenue"]).mark_bar().encode(
            x=alt.X("R_NAME:N", title="지역", sort="-y"),
            y=alt.Y("REVENUE:Q", title="매출 ($)", axis=alt.Axis(format="~s")),
            color=alt.Color("R_NAME:N", legend=None),
            tooltip=["R_NAME", alt.Tooltip("REVENUE:Q", format="$,.0f"), "ORDERS"]
        ).properties(height=300)
        st.altair_chart(region_chart, use_container_width=True)
    
    with col2:
        st.header("🏢 시장 세그먼트별 분석")
        segment_chart = alt.Chart(data["segment_analysis"]).mark_bar().encode(
            x=alt.X("C_MKTSEGMENT:N", title="세그먼트", sort="-y"),
            y=alt.Y("REVENUE:Q", title="매출 ($)", axis=alt.Axis(format="~s")),
            color=alt.Color("C_MKTSEGMENT:N", legend=None),
            tooltip=["C_MKTSEGMENT", alt.Tooltip("REVENUE:Q", format="$,.0f"), "ORDERS", alt.Tooltip("AVG_ORDER:Q", format="$,.2f")]
        ).properties(height=300)
        st.altair_chart(segment_chart, use_container_width=True)
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.header("🚨 주문 우선순위 분포")
        priority_chart = alt.Chart(data["priority_analysis"]).mark_arc(innerRadius=50).encode(
            theta=alt.Theta("ORDERS:Q"),
            color=alt.Color("O_ORDERPRIORITY:N", title="우선순위"),
            tooltip=["O_ORDERPRIORITY", "ORDERS", alt.Tooltip("REVENUE:Q", format="$,.0f")]
        ).properties(height=300)
        st.altair_chart(priority_chart, use_container_width=True)
    
    with col2:
        st.header("🏆 Top 10 국가별 매출")
        nation_chart = alt.Chart(data["nation_revenue"]).mark_bar().encode(
            x=alt.X("REVENUE:Q", title="매출 ($)", axis=alt.Axis(format="~s")),
            y=alt.Y("N_NAME:N", title="국가", sort="-x"),
            color=alt.Color("R_NAME:N", title="지역"),
            tooltip=["N_NAME", "R_NAME", alt.Tooltip("REVENUE:Q", format="$,.0f")]
        ).properties(height=300)
        st.altair_chart(nation_chart, use_container_width=True)
    
    st.divider()
    
    with st.expander("📋 Raw Data - 지역별 매출"):
        st.dataframe(data["region_revenue"], use_container_width=True)
    
    with st.expander("📋 Raw Data - 세그먼트별 분석"):
        st.dataframe(data["segment_analysis"], use_container_width=True)

else:
    st.warning("지역과 날짜 범위를 선택해주세요.")

st.sidebar.divider()
st.sidebar.markdown("**Data Source**: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1")
st.sidebar.markdown("Built with ❄️ Streamlit in Snowflake")
