"""Deep analysis for executive report on BJ's Club data."""
import os
import json
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()

CATALOG = os.getenv("CATALOG_NAME")
SCHEMA = os.getenv("SCHEMA_NAME")
DATABRICKS_HOST = os.getenv("DATABRICKS_BASE_URL")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Get warehouse
warehouses = list(w.warehouses.list())
wh = next((x for x in warehouses if x.state.value == "RUNNING"), warehouses[0])

def run_sql(sql):
    import time
    response = w.statement_execution.execute_statement(
        warehouse_id=wh.id,
        statement=sql,
        wait_timeout="50s"
    )
    while response.status.state.value in ["PENDING", "RUNNING"]:
        time.sleep(1)
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state.value == "FAILED":
        print(f"ERROR: {response.status.error}")
        return None
    return response

def get_results(response):
    if response and response.result and response.result.data_array:
        return response.result.data_array
    return []

print("=" * 80)
print("BJ'S WHOLESALE CLUB - EXECUTIVE ANALYTICS DEEP DIVE")
print("=" * 80)

# =============================================================================
# 1. REVENUE OVERVIEW & TRENDS
# =============================================================================
print("\n" + "=" * 80)
print("1. REVENUE OVERVIEW & TRENDS")
print("=" * 80)

# Overall metrics
overall = run_sql(f"""
SELECT
    SUM(revenue) as total_revenue,
    SUM(transaction_count) as total_transactions,
    SUM(units_sold) as total_units,
    SUM(revenue) / SUM(transaction_count) as avg_transaction_value,
    COUNT(DISTINCT store_id) as num_stores,
    COUNT(DISTINCT month) as num_months
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
""")
row = get_results(overall)[0]
print(f"\nOverall Performance (18 months):")
print(f"  Total Revenue:        ${float(row[0]):>15,.2f}")
print(f"  Total Transactions:   {int(row[1]):>15,}")
print(f"  Total Units Sold:     {int(row[2]):>15,}")
print(f"  Avg Transaction:      ${float(row[3]):>15,.2f}")
print(f"  Active Stores:        {row[4]}")
print(f"  Months of Data:       {row[5]}")

# Monthly revenue trend
monthly_trend = run_sql(f"""
SELECT
    month,
    SUM(revenue) as monthly_revenue,
    SUM(transaction_count) as monthly_transactions,
    SUM(revenue) / SUM(transaction_count) as avg_basket
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY month
ORDER BY month
""")
print(f"\nMonthly Revenue Trend:")
print(f"  {'Month':<12} {'Revenue':>18} {'Transactions':>15} {'Avg Basket':>12}")
print(f"  {'-'*12} {'-'*18} {'-'*15} {'-'*12}")
for row in get_results(monthly_trend):
    print(f"  {row[0]:<12} ${float(row[1]):>16,.0f} {int(row[2]):>15,} ${float(row[3]):>10,.2f}")

# YoY comparison (comparing same months in 2024 vs 2025)
yoy = run_sql(f"""
WITH monthly_data AS (
    SELECT
        YEAR(month) as year,
        MONTH(month) as month_num,
        SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    GROUP BY YEAR(month), MONTH(month)
)
SELECT
    m2025.month_num,
    m2024.revenue as revenue_2024,
    m2025.revenue as revenue_2025,
    ((m2025.revenue - m2024.revenue) / m2024.revenue) * 100 as yoy_growth
FROM monthly_data m2025
JOIN monthly_data m2024 ON m2025.month_num = m2024.month_num
WHERE m2025.year = 2025 AND m2024.year = 2024
ORDER BY m2025.month_num
""")
print(f"\nYear-over-Year Growth (2024 vs 2025):")
print(f"  {'Month':>6} {'2024 Revenue':>18} {'2025 Revenue':>18} {'YoY Growth':>12}")
print(f"  {'-'*6} {'-'*18} {'-'*18} {'-'*12}")
for row in get_results(yoy):
    month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    print(f"  {month_names[int(row[0])]:>6} ${float(row[1]):>16,.0f} ${float(row[2]):>16,.0f} {float(row[3]):>10.1f}%")

# Quarterly performance
quarterly = run_sql(f"""
SELECT
    CONCAT(YEAR(month), '-Q', QUARTER(month)) as quarter,
    SUM(revenue) as quarterly_revenue,
    SUM(transaction_count) as quarterly_transactions
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY CONCAT(YEAR(month), '-Q', QUARTER(month))
ORDER BY quarter
""")
print(f"\nQuarterly Performance:")
print(f"  {'Quarter':<10} {'Revenue':>18} {'Transactions':>15}")
print(f"  {'-'*10} {'-'*18} {'-'*15}")
for row in get_results(quarterly):
    print(f"  {row[0]:<10} ${float(row[1]):>16,.0f} {int(row[2]):>15,}")

# =============================================================================
# 2. STORE PERFORMANCE ANALYSIS
# =============================================================================
print("\n" + "=" * 80)
print("2. STORE PERFORMANCE ANALYSIS")
print("=" * 80)

# Top 10 stores by revenue
top_stores = run_sql(f"""
SELECT
    store_id,
    SUM(revenue) as total_revenue,
    SUM(transaction_count) as total_transactions,
    SUM(revenue) / SUM(transaction_count) as avg_basket,
    SUM(units_sold) / SUM(transaction_count) as units_per_transaction
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY store_id
ORDER BY total_revenue DESC
LIMIT 10
""")
print(f"\nTop 10 Stores by Revenue:")
print(f"  {'Store':>8} {'Revenue':>18} {'Transactions':>15} {'Avg Basket':>12} {'Units/Txn':>10}")
print(f"  {'-'*8} {'-'*18} {'-'*15} {'-'*12} {'-'*10}")
for row in get_results(top_stores):
    print(f"  {row[0]:>8} ${float(row[1]):>16,.0f} {int(row[2]):>15,} ${float(row[3]):>10,.2f} {float(row[4]):>10.1f}")

# Bottom 5 stores
bottom_stores = run_sql(f"""
SELECT
    store_id,
    SUM(revenue) as total_revenue,
    SUM(transaction_count) as total_transactions,
    SUM(revenue) / SUM(transaction_count) as avg_basket
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY store_id
ORDER BY total_revenue ASC
LIMIT 5
""")
print(f"\nBottom 5 Stores (Underperforming):")
print(f"  {'Store':>8} {'Revenue':>18} {'Transactions':>15} {'Avg Basket':>12}")
print(f"  {'-'*8} {'-'*18} {'-'*15} {'-'*12}")
for row in get_results(bottom_stores):
    print(f"  {row[0]:>8} ${float(row[1]):>16,.0f} {int(row[2]):>15,} ${float(row[3]):>10,.2f}")

# Store performance variance
store_variance = run_sql(f"""
SELECT
    AVG(store_revenue) as avg_store_revenue,
    STDDEV(store_revenue) as stddev_revenue,
    MIN(store_revenue) as min_revenue,
    MAX(store_revenue) as max_revenue,
    (MAX(store_revenue) - MIN(store_revenue)) / AVG(store_revenue) * 100 as revenue_spread_pct
FROM (
    SELECT store_id, SUM(revenue) as store_revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    GROUP BY store_id
)
""")
row = get_results(store_variance)[0]
print(f"\nStore Performance Variance:")
print(f"  Average Store Revenue:  ${float(row[0]):>15,.2f}")
print(f"  Std Deviation:          ${float(row[1]):>15,.2f}")
print(f"  Min Store Revenue:      ${float(row[2]):>15,.2f}")
print(f"  Max Store Revenue:      ${float(row[3]):>15,.2f}")
print(f"  Revenue Spread:         {float(row[4]):>15.1f}%")

# =============================================================================
# 3. CATEGORY PERFORMANCE
# =============================================================================
print("\n" + "=" * 80)
print("3. CATEGORY PERFORMANCE")
print("=" * 80)

# Category breakdown
category_perf = run_sql(f"""
SELECT
    category,
    SUM(revenue) as total_revenue,
    SUM(revenue) / (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly) * 100 as revenue_share,
    SUM(transaction_count) as transactions,
    SUM(revenue) / SUM(transaction_count) as avg_transaction,
    SUM(units_sold) as units
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY category
ORDER BY total_revenue DESC
""")
print(f"\nCategory Performance:")
print(f"  {'Category':<20} {'Revenue':>18} {'Share':>8} {'Transactions':>14} {'Avg Txn':>10}")
print(f"  {'-'*20} {'-'*18} {'-'*8} {'-'*14} {'-'*10}")
for row in get_results(category_perf):
    print(f"  {row[0]:<20} ${float(row[1]):>16,.0f} {float(row[2]):>7.1f}% {int(row[3]):>14,} ${float(row[4]):>8,.2f}")

# Category growth trends (comparing first 6 months vs last 6 months)
category_growth = run_sql(f"""
WITH first_half AS (
    SELECT category, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    WHERE month < '2025-05-01'
    GROUP BY category
),
second_half AS (
    SELECT category, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    WHERE month >= '2025-05-01'
    GROUP BY category
)
SELECT
    f.category,
    f.revenue as first_period,
    s.revenue as second_period,
    ((s.revenue - f.revenue) / f.revenue) * 100 as growth_rate
FROM first_half f
JOIN second_half s ON f.category = s.category
ORDER BY growth_rate DESC
""")
print(f"\nCategory Growth (H1 vs H2):")
print(f"  {'Category':<20} {'First Period':>16} {'Second Period':>16} {'Growth':>10}")
print(f"  {'-'*20} {'-'*16} {'-'*16} {'-'*10}")
for row in get_results(category_growth):
    print(f"  {row[0]:<20} ${float(row[1]):>14,.0f} ${float(row[2]):>14,.0f} {float(row[3]):>9.1f}%")

# =============================================================================
# 4. CUSTOMER ANALYSIS
# =============================================================================
print("\n" + "=" * 80)
print("4. CUSTOMER ANALYSIS")
print("=" * 80)

# Customer overview
cust_overview = run_sql(f"""
SELECT
    COUNT(*) as total_customers,
    SUM(lifetime_spend) as total_lifetime_spend,
    AVG(lifetime_spend) as avg_lifetime_spend,
    AVG(total_visits) as avg_visits,
    AVG(avg_basket_size) as avg_basket
FROM {CATALOG}.{SCHEMA}.customer_golden_view
""")
row = get_results(cust_overview)[0]
print(f"\nCustomer Overview:")
print(f"  Total Customers:        {int(row[0]):>15,}")
print(f"  Total Lifetime Spend:   ${float(row[1]):>15,.2f}")
print(f"  Avg Lifetime Spend:     ${float(row[2]):>15,.2f}")
print(f"  Avg Visits per Customer:{float(row[3]):>15.1f}")
print(f"  Avg Basket Size:        ${float(row[4]):>15,.2f}")

# Tier breakdown
tier_analysis = run_sql(f"""
SELECT
    membership_tier,
    COUNT(*) as customer_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as pct_of_customers,
    SUM(lifetime_spend) as total_spend,
    SUM(lifetime_spend) * 100.0 / SUM(SUM(lifetime_spend)) OVER() as pct_of_revenue,
    AVG(lifetime_spend) as avg_lifetime_spend,
    AVG(total_visits) as avg_visits,
    AVG(avg_basket_size) as avg_basket
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY membership_tier
ORDER BY total_spend DESC
""")
print(f"\nMembership Tier Analysis:")
print(f"  {'Tier':<15} {'Customers':>10} {'% Cust':>8} {'Total Spend':>16} {'% Rev':>8} {'Avg LTV':>12} {'Avg Visits':>10}")
print(f"  {'-'*15} {'-'*10} {'-'*8} {'-'*16} {'-'*8} {'-'*12} {'-'*10}")
for row in get_results(tier_analysis):
    print(f"  {row[0]:<15} {int(row[1]):>10,} {float(row[2]):>7.1f}% ${float(row[3]):>14,.0f} {float(row[4]):>7.1f}% ${float(row[5]):>10,.0f} {float(row[6]):>10.1f}")

# Customer tenure analysis
tenure_analysis = run_sql(f"""
SELECT
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 1 THEN '< 1 year'
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 3 THEN '1-3 years'
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 5 THEN '3-5 years'
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 10 THEN '5-10 years'
        ELSE '10+ years'
    END as tenure_bucket,
    COUNT(*) as customers,
    AVG(lifetime_spend) as avg_ltv,
    AVG(total_visits) as avg_visits
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY 1
ORDER BY
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 1 THEN 1
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 3 THEN 2
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 5 THEN 3
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 10 THEN 4
        ELSE 5
    END
""")
print(f"\nCustomer Tenure Analysis:")
print(f"  {'Tenure':<12} {'Customers':>10} {'Avg LTV':>14} {'Avg Visits':>12}")
print(f"  {'-'*12} {'-'*10} {'-'*14} {'-'*12}")
for row in get_results(tenure_analysis):
    print(f"  {row[0]:<12} {int(row[1]):>10,} ${float(row[2]):>12,.2f} {float(row[3]):>12.1f}")

# =============================================================================
# 5. TOP CUSTOMERS & VALUE CONCENTRATION
# =============================================================================
print("\n" + "=" * 80)
print("5. TOP CUSTOMERS & VALUE CONCENTRATION")
print("=" * 80)

# Top 20 customers
top_customers = run_sql(f"""
SELECT
    customer_id,
    first_name,
    last_name,
    membership_tier,
    home_club_id,
    lifetime_spend,
    total_visits,
    avg_basket_size
FROM {CATALOG}.{SCHEMA}.customer_golden_view
ORDER BY lifetime_spend DESC
LIMIT 20
""")
print(f"\nTop 20 Customers by Lifetime Value:")
print(f"  {'Customer ID':<14} {'Name':<25} {'Tier':<15} {'Club':>6} {'LTV':>14} {'Visits':>8}")
print(f"  {'-'*14} {'-'*25} {'-'*15} {'-'*6} {'-'*14} {'-'*8}")
for row in get_results(top_customers):
    name = f"{row[1]} {row[2]}"[:24]
    print(f"  {row[0]:<14} {name:<25} {row[3]:<15} {row[4]:>6} ${float(row[5]):>12,.2f} {int(row[6]):>8}")

# Revenue concentration
concentration = run_sql(f"""
WITH ranked AS (
    SELECT
        customer_id,
        lifetime_spend,
        SUM(lifetime_spend) OVER (ORDER BY lifetime_spend DESC) as cumulative_spend,
        SUM(lifetime_spend) OVER () as total_spend,
        ROW_NUMBER() OVER (ORDER BY lifetime_spend DESC) as rank,
        COUNT(*) OVER () as total_customers
    FROM {CATALOG}.{SCHEMA}.customer_golden_view
)
SELECT
    CASE
        WHEN rank <= total_customers * 0.01 THEN 'Top 1%'
        WHEN rank <= total_customers * 0.05 THEN 'Top 5%'
        WHEN rank <= total_customers * 0.10 THEN 'Top 10%'
        WHEN rank <= total_customers * 0.20 THEN 'Top 20%'
        ELSE 'Bottom 80%'
    END as segment,
    COUNT(*) as customers,
    SUM(lifetime_spend) as segment_spend,
    SUM(lifetime_spend) / (SELECT SUM(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view) * 100 as pct_of_total
FROM ranked
GROUP BY 1
ORDER BY MIN(rank)
""")
print(f"\nRevenue Concentration Analysis:")
print(f"  {'Segment':<15} {'Customers':>10} {'Total Spend':>18} {'% of Revenue':>14}")
print(f"  {'-'*15} {'-'*10} {'-'*18} {'-'*14}")
for row in get_results(concentration):
    print(f"  {row[0]:<15} {int(row[1]):>10,} ${float(row[2]):>16,.2f} {float(row[3]):>13.1f}%")

# =============================================================================
# 6. CUSTOMER ACTIVITY & CHURN RISK
# =============================================================================
print("\n" + "=" * 80)
print("6. CUSTOMER ACTIVITY & CHURN RISK")
print("=" * 80)

# Activity recency
activity = run_sql(f"""
SELECT
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 30 THEN 'Active (0-30 days)'
        WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 60 THEN 'Recent (31-60 days)'
        WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 90 THEN 'At Risk (61-90 days)'
        WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 180 THEN 'Churning (91-180 days)'
        ELSE 'Churned (180+ days)'
    END as activity_status,
    COUNT(*) as customers,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as pct,
    AVG(lifetime_spend) as avg_ltv,
    SUM(lifetime_spend) as total_at_risk_value
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY 1
ORDER BY MIN(DATEDIFF(CURRENT_DATE(), last_visit_date))
""")
print(f"\nCustomer Activity Status:")
print(f"  {'Status':<25} {'Customers':>10} {'%':>8} {'Avg LTV':>12} {'Value at Risk':>16}")
print(f"  {'-'*25} {'-'*10} {'-'*8} {'-'*12} {'-'*16}")
for row in get_results(activity):
    print(f"  {row[0]:<25} {int(row[1]):>10,} {float(row[2]):>7.1f}% ${float(row[3]):>10,.0f} ${float(row[4]):>14,.0f}")

# High-value at-risk customers
at_risk = run_sql(f"""
SELECT
    customer_id,
    first_name,
    last_name,
    membership_tier,
    lifetime_spend,
    last_visit_date,
    DATEDIFF(CURRENT_DATE(), last_visit_date) as days_since_visit
FROM {CATALOG}.{SCHEMA}.customer_golden_view
WHERE DATEDIFF(CURRENT_DATE(), last_visit_date) > 60
  AND lifetime_spend > 30000
ORDER BY lifetime_spend DESC
LIMIT 10
""")
print(f"\nHigh-Value At-Risk Customers (>$30K LTV, >60 days inactive):")
print(f"  {'Customer ID':<14} {'Name':<25} {'Tier':<15} {'LTV':>12} {'Days Inactive':>14}")
print(f"  {'-'*14} {'-'*25} {'-'*15} {'-'*12} {'-'*14}")
for row in get_results(at_risk):
    name = f"{row[1]} {row[2]}"[:24]
    print(f"  {row[0]:<14} {name:<25} {row[3]:<15} ${float(row[4]):>10,.0f} {int(row[6]):>14}")

# =============================================================================
# 7. CATEGORY SPENDING BY CUSTOMER SEGMENT
# =============================================================================
print("\n" + "=" * 80)
print("7. CATEGORY SPENDING BY CUSTOMER SEGMENT")
print("=" * 80)

# Category preference by tier
category_by_tier = run_sql(f"""
SELECT
    membership_tier,
    ROUND(AVG(spend_grocery), 2) as grocery,
    ROUND(AVG(spend_meat_seafood), 2) as meat_seafood,
    ROUND(AVG(spend_electronics), 2) as electronics,
    ROUND(AVG(spend_household), 2) as household,
    ROUND(AVG(spend_beverages), 2) as beverages
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY membership_tier
ORDER BY AVG(lifetime_spend) DESC
""")
print(f"\nAverage Category Spend by Membership Tier:")
print(f"  {'Tier':<15} {'Grocery':>12} {'Meat/Seafood':>14} {'Electronics':>12} {'Household':>12} {'Beverages':>12}")
print(f"  {'-'*15} {'-'*12} {'-'*14} {'-'*12} {'-'*12} {'-'*12}")
for row in get_results(category_by_tier):
    print(f"  {row[0]:<15} ${float(row[1]):>10,.0f} ${float(row[2]):>12,.0f} ${float(row[3]):>10,.0f} ${float(row[4]):>10,.0f} ${float(row[5]):>10,.0f}")

# =============================================================================
# 8. STORE-CUSTOMER RELATIONSHIP
# =============================================================================
print("\n" + "=" * 80)
print("8. STORE-CUSTOMER RELATIONSHIP (Home Club Analysis)")
print("=" * 80)

# Customers per store
store_customers = run_sql(f"""
SELECT
    c.home_club_id as store_id,
    COUNT(*) as customer_count,
    AVG(c.lifetime_spend) as avg_customer_ltv,
    SUM(c.lifetime_spend) as total_customer_value
FROM {CATALOG}.{SCHEMA}.customer_golden_view c
GROUP BY c.home_club_id
ORDER BY total_customer_value DESC
LIMIT 10
""")
print(f"\nTop 10 Stores by Customer Base Value:")
print(f"  {'Store':>8} {'Customers':>12} {'Avg LTV':>14} {'Total Value':>18}")
print(f"  {'-'*8} {'-'*12} {'-'*14} {'-'*18}")
for row in get_results(store_customers):
    print(f"  {row[0]:>8} {int(row[1]):>12,} ${float(row[2]):>12,.2f} ${float(row[3]):>16,.2f}")

# =============================================================================
# 9. SEASONALITY INSIGHTS
# =============================================================================
print("\n" + "=" * 80)
print("9. SEASONALITY INSIGHTS")
print("=" * 80)

# Monthly seasonality index
seasonality = run_sql(f"""
WITH monthly_avg AS (
    SELECT AVG(monthly_rev) as avg_monthly
    FROM (
        SELECT month, SUM(revenue) as monthly_rev
        FROM {CATALOG}.{SCHEMA}.store_sales_monthly
        GROUP BY month
    )
),
monthly_totals AS (
    SELECT
        MONTH(month) as month_num,
        AVG(monthly_rev) as avg_for_month
    FROM (
        SELECT month, SUM(revenue) as monthly_rev
        FROM {CATALOG}.{SCHEMA}.store_sales_monthly
        GROUP BY month
    )
    GROUP BY MONTH(month)
)
SELECT
    month_num,
    avg_for_month,
    avg_for_month / (SELECT avg_monthly FROM monthly_avg) * 100 as seasonality_index
FROM monthly_totals
ORDER BY month_num
""")
print(f"\nSeasonality Index by Month (100 = average):")
month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
print(f"  {'Month':<6} {'Avg Revenue':>16} {'Index':>10}")
print(f"  {'-'*6} {'-'*16} {'-'*10}")
for row in get_results(seasonality):
    idx = float(row[2])
    bar = "█" * int(idx / 10)
    print(f"  {month_names[int(row[0])]:<6} ${float(row[1]):>14,.0f} {idx:>8.0f} {bar}")

# =============================================================================
# 10. KEY METRICS SUMMARY
# =============================================================================
print("\n" + "=" * 80)
print("10. KEY METRICS SUMMARY (EXECUTIVE DASHBOARD)")
print("=" * 80)

summary = run_sql(f"""
SELECT
    -- Revenue metrics
    (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly) as total_revenue,
    (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly WHERE YEAR(month) = 2025) as revenue_2025,
    (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly WHERE YEAR(month) = 2024) as revenue_2024,

    -- Customer metrics
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.customer_golden_view) as total_customers,
    (SELECT AVG(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view) as avg_ltv,
    (SELECT SUM(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view WHERE membership_tier = 'Inner Circle') /
    (SELECT SUM(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view) * 100 as inner_circle_revenue_pct,

    -- Activity metrics
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.customer_golden_view WHERE DATEDIFF(CURRENT_DATE(), last_visit_date) <= 30) as active_customers,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.customer_golden_view WHERE DATEDIFF(CURRENT_DATE(), last_visit_date) > 90) as at_risk_customers
""")
row = get_results(summary)[0]
print(f"""
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  REVENUE                                                                │
  │    Total (18 mo):     ${float(row[0]):>14,.0f}                          │
  │    2025 YTD:          ${float(row[1]):>14,.0f}                          │
  │    2024 Total:        ${float(row[2]):>14,.0f}                          │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  CUSTOMERS                                                              │
  │    Total Members:     {int(row[3]):>14,}                                │
  │    Avg Lifetime Value:${float(row[4]):>14,.2f}                          │
  │    Inner Circle Rev %:{float(row[5]):>14.1f}%                           │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  ENGAGEMENT                                                             │
  │    Active (30 days):  {int(row[6]):>14,}                                │
  │    At Risk (>90 days):{int(row[7]):>14,}                                │
  └─────────────────────────────────────────────────────────────────────────┘
""")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
