import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="📊 Business KPI Dashboard", layout="wide")
st.title("📊 Food Delivery KPI Dashboard")

session = get_active_session()

def run_query(query):
    return pd.DataFrame(session.sql(query).collect())

# Utility
def format_inr(value): return f"₹{value:,.0f}"

# 1. Yearly Revenue KPIs
yearly_df = run_query("""
    SELECT * FROM consumption_sch.vw_yearly_revenue_kpis ORDER BY year;
""")

years = yearly_df["YEAR"].unique()
default_year = max(years)
selected_year = st.selectbox("📅 Select Year", sorted(years), index=list(years).index(default_year))
selected_data = yearly_df[yearly_df["YEAR"] == selected_year]

# Top-level KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Revenue", format_inr(selected_data["TOTAL_REVENUE"].iloc[0]))
with col2:
    st.metric("Total Orders", f"{selected_data['TOTAL_ORDERS'].iloc[0]:,}")
with col3:
    st.metric("Avg Revenue/Order", format_inr(selected_data["AVG_REVENUE_PER_ORDER"].iloc[0]))

st.divider()

# 2. Monthly Revenue Trend
monthly_df = run_query(f"""
    SELECT month, total_revenue FROM consumption_sch.vw_monthly_revenue_kpis 
    WHERE year = {selected_year} ORDER BY month;
""")
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
monthly_df["Month"] = monthly_df["MONTH"].apply(lambda x: month_names[x-1])

bar = alt.Chart(monthly_df).mark_bar(color="#FF6B35").encode(
    x=alt.X('Month', sort=month_names),
    y=alt.Y('TOTAL_REVENUE', title="Revenue (₹)"),
    tooltip=["Month", "TOTAL_REVENUE"]
).properties(width=700, height=300)

line = alt.Chart(monthly_df).mark_line(color="#FFA07A", point=True).encode(
    x='Month', y='TOTAL_REVENUE'
)

st.subheader("📈 Monthly Revenue Trend")
st.altair_chart(bar + line, use_container_width=True)

st.divider()

# 3. Tabs for KPI Exploration
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏆 Restaurants",
    "👥 Customers",
    "🍽️ Menu & Cuisine",
    "📍 Locations",
    "📦 Operational Metrics"
])

# === 4. Restaurants Tab ===
with tab1:
    months_df = run_query(f"""
        SELECT DISTINCT month FROM consumption_sch.vw_monthly_revenue_by_restaurant 
        WHERE year = {selected_year} ORDER BY month
    """)
    selected_month = st.selectbox("Select Month", sorted(months_df["MONTH"].tolist()), index=len(months_df)-1)

    rest_df = run_query(f"""
        SELECT restaurant_name, total_revenue, total_orders, avg_revenue_per_order 
        FROM consumption_sch.vw_monthly_revenue_by_restaurant 
        WHERE year = {selected_year} AND month = {selected_month}
        ORDER BY total_revenue DESC LIMIT 10;
    """)
    st.subheader(f"🏅 Top 10 Restaurants for {month_names[selected_month-1]} {selected_year}")
    st.dataframe(rest_df.style.format({
        "total_revenue": "₹{:,.0f}",
        "avg_revenue_per_order": "₹{:,.0f}"
    }), use_container_width=True)

# === 5. Customers Tab ===
with tab2:
    col1, col2 = st.columns(2)

    repeat_df = run_query("SELECT * FROM consumption_sch.vw_repeat_customer_rate")
    clv_df = run_query("SELECT * FROM consumption_sch.vw_avg_customer_lifetime_value")

    with col1:
        st.metric("Repeat Customer Rate", f"{repeat_df['REPEAT_CUSTOMER_RATE'].iloc[0]*100:.1f}%")
    with col2:
        st.metric("Avg Customer Lifetime Value", format_inr(clv_df['AVG_CUSTOMER_LIFETIME_VALUE'].iloc[0]))

    st.subheader("👑 Top Customers by Revenue")
    top_cust_df = run_query("SELECT * FROM consumption_sch.vw_top_customers_by_revenue")
    st.dataframe(top_cust_df.style.format({
        "total_spent": "₹{:,.0f}"
    }), use_container_width=True)

    new_ret_df = run_query("SELECT * FROM consumption_sch.vw_customer_type_summary")
    st.subheader("📊 New vs Returning Customers by Year")
    st.bar_chart(new_ret_df.set_index("YEAR"))

# === 6. Menu Tab ===
with tab3:
    top_menu_df = run_query("SELECT * FROM consumption_sch.vw_top_menu_items")
    st.subheader("🍔 Top Menu Items by Quantity Sold")
    st.dataframe(top_menu_df.style.format({
        "total_revenue_generated": "₹{:,.0f}"
    }), use_container_width=True)

    cuisine_df = run_query("SELECT * FROM consumption_sch.vw_revenue_by_cuisine")
    st.subheader("🍱 Revenue by Cuisine Type")
    st.bar_chart(cuisine_df.set_index("CUISINE_TYPE"))

    itemtype_df = run_query("SELECT * FROM consumption_sch.vw_revenue_by_item_type")
    st.subheader("🍗 Revenue by Item Type")
    st.bar_chart(itemtype_df.set_index("ITEM_TYPE"))

    cat_df = run_query("SELECT * FROM consumption_sch.vw_revenue_by_menu_category")
    st.subheader("📂 Revenue by Menu Category")
    st.bar_chart(cat_df.set_index("CATEGORY"))

# === 7. Location Tab ===
with tab4:
    loc_df = run_query("SELECT * FROM consumption_sch.vw_revenue_by_locality")
    st.subheader("📍 Revenue by Locality")
    st.bar_chart(loc_df.set_index("LOCALITY"))

    city_df = run_query("SELECT * FROM consumption_sch.vw_top_restaurant_locations")
    st.subheader("🏙️ Top Cities by Restaurant Revenue")
    st.bar_chart(city_df.set_index("LOCATION_CITY"))

# === 8. Ops Tab ===
with tab5:
    dow_df = run_query("SELECT * FROM consumption_sch.vw_revenue_by_weekday")
    st.subheader("📆 Revenue by Day of Week")
    st.bar_chart(dow_df.set_index("DAY_NAME"))

    peak_day_df = run_query("SELECT * FROM consumption_sch.vw_peak_order_day")
    st.metric("📈 Peak Order Day", peak_day_df["DAY_NAME"].iloc[0])

    loss_df = run_query("SELECT * FROM consumption_sch.vw_lost_revenue_cancelled")
    st.metric("💸 Lost Revenue from Cancelled Orders", format_inr(loss_df["LOST_REVENUE"].iloc[0]))

    gender_df = run_query("SELECT * FROM consumption_sch.vw_orders_by_gender")
    st.subheader("🧑‍🤝‍🧑 Orders by Gender")
    st.bar_chart(gender_df.set_index("GENDER"))

    avg_items_df = run_query("SELECT * FROM consumption_sch.vw_avg_items_per_order")
    st.metric("🛍️ Avg Items per Order", float(avg_items_df['AVG_ITEMS_PER_ORDER'].iloc[0]))

    agent_df = run_query("SELECT * FROM consumption_sch.vw_revenue_by_delivery_agent")
    st.subheader("🚚 Revenue by Delivery Agent")
    st.dataframe(agent_df.style.format({
        "total_revenue": "₹{:,.0f}"
    }), use_container_width=True)
