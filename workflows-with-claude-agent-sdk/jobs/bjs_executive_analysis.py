# Databricks notebook source
# MAGIC %md
# MAGIC # BJ's Wholesale Club - Executive Analysis
# MAGIC
# MAGIC This notebook runs comprehensive analytics on the BJ's data and creates summary tables
# MAGIC for executive reporting and dashboards.

# COMMAND ----------

# Parameters
try:
    CATALOG = dbutils.widgets.get("catalog_name")
except Exception:
    CATALOG = "serverless_9cefok_catalog"

try:
    SCHEMA = dbutils.widgets.get("schema_name")
except Exception:
    SCHEMA = "sgfs"

print(f"Analyzing data in {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Revenue Overview

# COMMAND ----------

# Overall metrics
overall_metrics = spark.sql(f"""
SELECT
    SUM(revenue) as total_revenue,
    SUM(transaction_count) as total_transactions,
    SUM(units_sold) as total_units,
    SUM(revenue) / SUM(transaction_count) as avg_transaction_value,
    COUNT(DISTINCT store_id) as num_stores,
    COUNT(DISTINCT month) as num_months,
    MIN(month) as start_date,
    MAX(month) as end_date
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
""")
overall_metrics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Monthly Revenue Trend

# COMMAND ----------

monthly_trend = spark.sql(f"""
SELECT
    month,
    SUM(revenue) as monthly_revenue,
    SUM(transaction_count) as monthly_transactions,
    SUM(revenue) / SUM(transaction_count) as avg_basket
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY month
ORDER BY month
""")
monthly_trend.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Year-over-Year Comparison

# COMMAND ----------

yoy_comparison = spark.sql(f"""
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
    ROUND(((m2025.revenue - m2024.revenue) / m2024.revenue) * 100, 2) as yoy_growth_pct
FROM monthly_data m2025
JOIN monthly_data m2024 ON m2025.month_num = m2024.month_num
WHERE m2025.year = 2025 AND m2024.year = 2024
ORDER BY m2025.month_num
""")
yoy_comparison.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Store Performance

# COMMAND ----------

store_performance = spark.sql(f"""
SELECT
    store_id,
    SUM(revenue) as total_revenue,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(revenue) / SUM(transaction_count), 2) as avg_basket,
    ROUND(SUM(units_sold) / SUM(transaction_count), 2) as units_per_transaction,
    RANK() OVER (ORDER BY SUM(revenue) DESC) as revenue_rank
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY store_id
ORDER BY total_revenue DESC
""")
store_performance.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Category Performance

# COMMAND ----------

category_performance = spark.sql(f"""
SELECT
    category,
    SUM(revenue) as total_revenue,
    ROUND(SUM(revenue) / (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly) * 100, 2) as revenue_share_pct,
    SUM(transaction_count) as transactions,
    ROUND(SUM(revenue) / SUM(transaction_count), 2) as avg_transaction
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY category
ORDER BY total_revenue DESC
""")
category_performance.display()

# COMMAND ----------

# Category growth (first half vs second half)
category_growth = spark.sql(f"""
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
    ROUND(f.revenue, 2) as first_period_revenue,
    ROUND(s.revenue, 2) as second_period_revenue,
    ROUND(((s.revenue - f.revenue) / f.revenue) * 100, 2) as growth_rate_pct
FROM first_half f
JOIN second_half s ON f.category = s.category
ORDER BY growth_rate_pct DESC
""")
category_growth.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Customer Analysis

# COMMAND ----------

customer_overview = spark.sql(f"""
SELECT
    COUNT(*) as total_customers,
    ROUND(SUM(lifetime_spend), 2) as total_lifetime_spend,
    ROUND(AVG(lifetime_spend), 2) as avg_lifetime_spend,
    ROUND(AVG(total_visits), 1) as avg_visits,
    ROUND(AVG(avg_basket_size), 2) as avg_basket
FROM {CATALOG}.{SCHEMA}.customer_golden_view
""")
customer_overview.display()

# COMMAND ----------

# Membership tier analysis
tier_analysis = spark.sql(f"""
SELECT
    membership_tier,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct_of_customers,
    ROUND(SUM(lifetime_spend), 2) as total_spend,
    ROUND(SUM(lifetime_spend) * 100.0 / SUM(SUM(lifetime_spend)) OVER(), 2) as pct_of_revenue,
    ROUND(AVG(lifetime_spend), 2) as avg_lifetime_spend,
    ROUND(AVG(total_visits), 1) as avg_visits
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY membership_tier
ORDER BY total_spend DESC
""")
tier_analysis.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Customer Activity & Churn Risk

# COMMAND ----------

activity_status = spark.sql(f"""
SELECT
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 30 THEN '1-Active (0-30 days)'
        WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 60 THEN '2-Recent (31-60 days)'
        WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 90 THEN '3-At Risk (61-90 days)'
        ELSE '4-Churning (90+ days)'
    END as activity_status,
    COUNT(*) as customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct,
    ROUND(AVG(lifetime_spend), 2) as avg_ltv,
    ROUND(SUM(lifetime_spend), 2) as total_value_at_risk
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY 1
ORDER BY 1
""")
activity_status.display()

# COMMAND ----------

# High-value at-risk customers
at_risk_customers = spark.sql(f"""
SELECT
    customer_id,
    first_name,
    last_name,
    membership_tier,
    ROUND(lifetime_spend, 2) as lifetime_spend,
    last_visit_date,
    DATEDIFF(CURRENT_DATE(), last_visit_date) as days_since_visit
FROM {CATALOG}.{SCHEMA}.customer_golden_view
WHERE DATEDIFF(CURRENT_DATE(), last_visit_date) > 60
  AND lifetime_spend > 30000
ORDER BY lifetime_spend DESC
LIMIT 20
""")
at_risk_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Revenue Concentration

# COMMAND ----------

revenue_concentration = spark.sql(f"""
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
    ROUND(SUM(lifetime_spend), 2) as segment_spend,
    ROUND(SUM(lifetime_spend) / (SELECT SUM(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view) * 100, 2) as pct_of_total
FROM ranked
GROUP BY 1
ORDER BY MIN(rank)
""")
revenue_concentration.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Seasonality Index

# COMMAND ----------

seasonality = spark.sql(f"""
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
    ROUND(avg_for_month, 2) as avg_revenue,
    ROUND(avg_for_month / (SELECT avg_monthly FROM monthly_avg) * 100, 1) as seasonality_index
FROM monthly_totals
ORDER BY month_num
""")
seasonality.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Save Summary Tables for Dashboards

# COMMAND ----------

print("Saving summary tables...")

# Executive summary
exec_summary = spark.sql(f"""
SELECT
    (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly) as total_revenue,
    (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly WHERE YEAR(month) = 2025) as revenue_2025,
    (SELECT SUM(revenue) FROM {CATALOG}.{SCHEMA}.store_sales_monthly WHERE YEAR(month) = 2024) as revenue_2024,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.customer_golden_view) as total_customers,
    (SELECT AVG(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view) as avg_ltv,
    (SELECT SUM(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view WHERE membership_tier = 'Inner Circle') /
    (SELECT SUM(lifetime_spend) FROM {CATALOG}.{SCHEMA}.customer_golden_view) * 100 as inner_circle_revenue_pct,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.customer_golden_view WHERE DATEDIFF(CURRENT_DATE(), last_visit_date) <= 30) as active_customers,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.customer_golden_view WHERE DATEDIFF(CURRENT_DATE(), last_visit_date) > 90) as at_risk_customers,
    CURRENT_TIMESTAMP() as report_generated_at
""")

exec_summary.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.exec_summary")
print("  Created exec_summary")

# Store performance summary
store_performance.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.store_performance_summary")
print("  Created store_performance_summary")

# Category performance summary
category_performance.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.category_performance_summary")
print("  Created category_performance_summary")

# Customer tier summary
tier_analysis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customer_tier_summary")
print("  Created customer_tier_summary")

# Churn risk summary
activity_status.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customer_activity_summary")
print("  Created customer_activity_summary")

# At-risk high-value customers
at_risk_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.high_value_at_risk_customers")
print("  Created high_value_at_risk_customers")

# Monthly trend
monthly_trend.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.monthly_revenue_trend")
print("  Created monthly_revenue_trend")

# Seasonality
seasonality.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.seasonality_index")
print("  Created seasonality_index")

print("\nAll summary tables created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Analysis Complete

# COMMAND ----------

print("=" * 60)
print("EXECUTIVE ANALYSIS COMPLETE")
print("=" * 60)

# Display final summary
spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.exec_summary").display()

print(f"""
Summary Tables Created:
  - {CATALOG}.{SCHEMA}.exec_summary
  - {CATALOG}.{SCHEMA}.store_performance_summary
  - {CATALOG}.{SCHEMA}.category_performance_summary
  - {CATALOG}.{SCHEMA}.customer_tier_summary
  - {CATALOG}.{SCHEMA}.customer_activity_summary
  - {CATALOG}.{SCHEMA}.high_value_at_risk_customers
  - {CATALOG}.{SCHEMA}.monthly_revenue_trend
  - {CATALOG}.{SCHEMA}.seasonality_index

These tables can be used for:
  - Databricks SQL Dashboards
  - BI Tool connections (Tableau, Power BI)
  - Executive reporting
  - AI-powered insights generation
""")
