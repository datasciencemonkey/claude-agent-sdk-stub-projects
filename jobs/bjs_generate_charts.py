# Databricks notebook source
# MAGIC %md
# MAGIC # BJ's Wholesale Club - Generate Executive Charts
# MAGIC
# MAGIC This notebook generates visualization charts from the analysis data.

# COMMAND ----------

# MAGIC %pip install matplotlib
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# Set style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['axes.titleweight'] = 'bold'

# Colors
COLORS = {
    'primary': '#1E3A5F',
    'secondary': '#3498db',
    'accent': '#e74c3c',
    'success': '#27ae60',
    'warning': '#f39c12',
    'neutral': '#95a5a6',
    'palette': ['#1E3A5F', '#3498db', '#27ae60', '#f39c12', '#e74c3c', '#9b59b6', '#1abc9c', '#34495e']
}

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

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")

# Volume path for storing charts
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/reports"

# Create volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.reports")
print(f"Charts will be saved to: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Monthly Revenue Trend

# COMMAND ----------

print("Generating Chart 1: Monthly Revenue Trend...")

data = spark.sql(f"""
    SELECT month, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    GROUP BY month ORDER BY month
""").collect()

months = [str(row.month)[:7] for row in data]
revenue = [float(row.revenue)/1e6 for row in data]

fig, ax = plt.subplots(figsize=(12, 5))
bars = ax.bar(months, revenue, color=COLORS['primary'], edgecolor='white', linewidth=0.5)

# Highlight peak months
for i, (m, r) in enumerate(zip(months, revenue)):
    if '12' in m or r > 115:
        bars[i].set_color(COLORS['success'])

ax.set_xlabel('Month')
ax.set_ylabel('Revenue ($ Millions)')
ax.set_title('Monthly Revenue Trend')
ax.set_ylim(0, max(revenue) * 1.15)

for i, (m, r) in enumerate(zip(months, revenue)):
    ax.text(i, r + 2, f'${r:.0f}M', ha='center', va='bottom', fontsize=8)

plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(f'{VOLUME_PATH}/01_monthly_revenue.png', dpi=150, bbox_inches='tight')
plt.close()
print("  Saved: 01_monthly_revenue.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Category Performance

# COMMAND ----------

print("Generating Chart 2: Category Performance...")

data = spark.sql(f"""
    SELECT category, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    GROUP BY category ORDER BY revenue DESC
""").collect()

categories = [row.category for row in data]
cat_revenue = [float(row.revenue)/1e6 for row in data]

fig, ax = plt.subplots(figsize=(10, 6))
y_pos = np.arange(len(categories))
bars = ax.barh(y_pos, cat_revenue, color=COLORS['palette'][:len(categories)])

ax.set_yticks(y_pos)
ax.set_yticklabels(categories)
ax.set_xlabel('Revenue ($ Millions)')
ax.set_title('Revenue by Category')
ax.invert_yaxis()

for i, r in enumerate(cat_revenue):
    pct = r / sum(cat_revenue) * 100
    ax.text(r + 2, i, f'${r:.0f}M ({pct:.1f}%)', va='center', fontsize=9)

ax.set_xlim(0, max(cat_revenue) * 1.25)
plt.tight_layout()
plt.savefig(f'{VOLUME_PATH}/02_category_revenue.png', dpi=150, bbox_inches='tight')
plt.close()
print("  Saved: 02_category_revenue.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Membership Tier Distribution

# COMMAND ----------

print("Generating Chart 3: Membership Tier Distribution...")

data = spark.sql(f"""
    SELECT membership_tier, COUNT(*) as cnt, SUM(lifetime_spend) as spend
    FROM {CATALOG}.{SCHEMA}.customer_golden_view
    GROUP BY membership_tier ORDER BY spend DESC
""").collect()

tiers = [row.membership_tier for row in data]
counts = [int(row.cnt) for row in data]
spend = [float(row.spend)/1e6 for row in data]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

colors_tier = [COLORS['primary'], COLORS['secondary'], COLORS['warning']]
wedges1, texts1, autotexts1 = ax1.pie(counts, labels=tiers, autopct='%1.1f%%',
                                       colors=colors_tier, startangle=90,
                                       wedgeprops=dict(width=0.6))
ax1.set_title('Customers by Tier')
centre_circle1 = plt.Circle((0, 0), 0.35, fc='white')
ax1.add_patch(centre_circle1)

wedges2, texts2, autotexts2 = ax2.pie(spend, labels=tiers, autopct='%1.1f%%',
                                       colors=colors_tier, startangle=90,
                                       wedgeprops=dict(width=0.6))
ax2.set_title('Revenue by Tier')
centre_circle2 = plt.Circle((0, 0), 0.35, fc='white')
ax2.add_patch(centre_circle2)

plt.suptitle('Membership Tier Analysis: Customers vs Revenue', fontsize=12, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig(f'{VOLUME_PATH}/03_membership_tiers.png', dpi=150, bbox_inches='tight')
plt.close()
print("  Saved: 03_membership_tiers.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Customer Activity Status

# COMMAND ----------

print("Generating Chart 4: Customer Activity Status...")

data = spark.sql(f"""
    SELECT
        CASE
            WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 30 THEN '1-Active (0-30d)'
            WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 60 THEN '2-Recent (31-60d)'
            WHEN DATEDIFF(CURRENT_DATE(), last_visit_date) <= 90 THEN '3-At Risk (61-90d)'
            ELSE '4-Churning (90+d)'
        END as status,
        COUNT(*) as cnt
    FROM {CATALOG}.{SCHEMA}.customer_golden_view
    GROUP BY 1 ORDER BY 1
""").collect()

statuses = [row.status.split('-')[1] for row in data]
counts = [int(row.cnt) for row in data]

fig, ax = plt.subplots(figsize=(8, 5))
colors_status = [COLORS['success'], COLORS['secondary'], COLORS['warning'], COLORS['accent']]
bars = ax.bar(statuses, counts, color=colors_status, edgecolor='white', linewidth=0.5)

ax.set_xlabel('Activity Status')
ax.set_ylabel('Number of Customers')
ax.set_title('Customer Engagement Status')

for i, c in enumerate(counts):
    pct = c / sum(counts) * 100
    ax.text(i, c + 50, f'{c:,}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=10)

ax.set_ylim(0, max(counts) * 1.2)
plt.tight_layout()
plt.savefig(f'{VOLUME_PATH}/04_customer_activity.png', dpi=150, bbox_inches='tight')
plt.close()
print("  Saved: 04_customer_activity.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Store Performance

# COMMAND ----------

print("Generating Chart 5: Store Performance...")

data = spark.sql(f"""
    SELECT store_id, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    GROUP BY store_id ORDER BY revenue DESC
""").collect()

stores = [str(row.store_id) for row in data]
store_rev = [float(row.revenue)/1e6 for row in data]
avg_rev = sum(store_rev) / len(store_rev)

fig, ax = plt.subplots(figsize=(12, 5))
colors_store = [COLORS['success'] if r > avg_rev else COLORS['accent'] for r in store_rev]
bars = ax.bar(stores, store_rev, color=colors_store, edgecolor='white', linewidth=0.5)

ax.axhline(y=avg_rev, color=COLORS['primary'], linestyle='--', linewidth=2, label=f'Average: ${avg_rev:.0f}M')
ax.set_xlabel('Store ID')
ax.set_ylabel('Revenue ($ Millions)')
ax.set_title('Store Performance Comparison (18 Months)')
ax.legend(loc='upper right')

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(f'{VOLUME_PATH}/05_store_performance.png', dpi=150, bbox_inches='tight')
plt.close()
print("  Saved: 05_store_performance.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Revenue Concentration (Pareto)

# COMMAND ----------

print("Generating Chart 6: Revenue Concentration...")

data = spark.sql(f"""
    SELECT lifetime_spend FROM {CATALOG}.{SCHEMA}.customer_golden_view ORDER BY lifetime_spend DESC
""").collect()

spend_values = [float(row.lifetime_spend) for row in data]
total = sum(spend_values)
cumsum = np.cumsum(spend_values) / total * 100
x_pct = np.arange(1, len(spend_values) + 1) / len(spend_values) * 100

fig, ax = plt.subplots(figsize=(10, 6))
ax.fill_between(x_pct, cumsum, alpha=0.3, color=COLORS['primary'])
ax.plot(x_pct, cumsum, color=COLORS['primary'], linewidth=2)

key_points = [(20, 60), (10, 40), (5, 24)]
for x, y in key_points:
    idx = int(len(spend_values) * x / 100)
    actual_y = cumsum[idx] if idx < len(cumsum) else cumsum[-1]
    ax.plot(x, actual_y, 'o', color=COLORS['accent'], markersize=10)
    ax.annotate(f'Top {x}% = {actual_y:.0f}% revenue', xy=(x, actual_y),
                xytext=(x + 10, actual_y - 5), fontsize=10,
                arrowprops=dict(arrowstyle='->', color=COLORS['neutral']))

ax.axhline(y=80, color=COLORS['warning'], linestyle='--', alpha=0.7, label='80% threshold')
ax.axvline(x=20, color=COLORS['warning'], linestyle='--', alpha=0.7)

ax.set_xlabel('% of Customers (ranked by value)')
ax.set_ylabel('Cumulative % of Revenue')
ax.set_title('Customer Revenue Concentration (Pareto Analysis)')
ax.set_xlim(0, 100)
ax.set_ylim(0, 105)
ax.legend(loc='lower right')

plt.tight_layout()
plt.savefig(f'{VOLUME_PATH}/06_revenue_concentration.png', dpi=150, bbox_inches='tight')
plt.close()
print("  Saved: 06_revenue_concentration.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Seasonality Index

# COMMAND ----------

print("Generating Chart 7: Seasonality Index...")

data = spark.sql(f"""
    WITH monthly_avg AS (
        SELECT AVG(monthly_rev) as avg_monthly
        FROM (SELECT month, SUM(revenue) as monthly_rev FROM {CATALOG}.{SCHEMA}.store_sales_monthly GROUP BY month)
    ),
    monthly_totals AS (
        SELECT MONTH(month) as month_num, AVG(monthly_rev) as avg_for_month
        FROM (SELECT month, SUM(revenue) as monthly_rev FROM {CATALOG}.{SCHEMA}.store_sales_monthly GROUP BY month)
        GROUP BY MONTH(month)
    )
    SELECT month_num, avg_for_month / (SELECT avg_monthly FROM monthly_avg) * 100 as idx
    FROM monthly_totals ORDER BY month_num
""").collect()

month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
indices = [float(row.idx) for row in data]

fig, ax = plt.subplots(figsize=(10, 5))
colors_season = [COLORS['accent'] if idx < 85 else COLORS['success'] if idx > 115 else COLORS['primary'] for idx in indices]
bars = ax.bar(month_names, indices, color=colors_season, edgecolor='white', linewidth=0.5)

ax.axhline(y=100, color=COLORS['neutral'], linestyle='--', linewidth=1.5, label='Average (100)')
ax.set_xlabel('Month')
ax.set_ylabel('Seasonality Index')
ax.set_title('Monthly Seasonality Index (100 = Average)')
ax.set_ylim(60, 145)

for i, idx in enumerate(indices):
    ax.text(i, idx + 2, f'{idx:.0f}', ha='center', va='bottom', fontsize=9)

ax.legend(loc='upper left')
plt.tight_layout()
plt.savefig(f'{VOLUME_PATH}/07_seasonality.png', dpi=150, bbox_inches='tight')
plt.close()
print("  Saved: 07_seasonality.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("CHART GENERATION COMPLETE")
print("=" * 60)
print(f"\nCharts saved to: {VOLUME_PATH}")
print("\nCharts generated:")
print("  1. Monthly Revenue Trend")
print("  2. Category Revenue")
print("  3. Membership Tiers")
print("  4. Customer Activity")
print("  5. Store Performance")
print("  6. Revenue Concentration")
print("  7. Seasonality Index")

# List files in volume
files = dbutils.fs.ls(VOLUME_PATH)
print(f"\nFiles in volume ({len(files)}):")
for f in files:
    print(f"  - {f.name}")
