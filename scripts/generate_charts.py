"""Generate executive report charts."""
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

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

load_dotenv()

CATALOG = os.getenv("CATALOG_NAME")
SCHEMA = os.getenv("SCHEMA_NAME")
DATABRICKS_HOST = os.getenv("DATABRICKS_BASE_URL")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

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
    if response.result and response.result.data_array:
        return response.result.data_array
    return []

# Create charts directory
os.makedirs("reports/charts", exist_ok=True)

print("Generating charts...")

# =============================================================================
# 1. MONTHLY REVENUE TREND
# =============================================================================
print("  1. Monthly Revenue Trend...")
data = run_sql(f"""
SELECT month, SUM(revenue) as revenue
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY month ORDER BY month
""")

months = [str(row[0])[:7] for row in data]
revenue = [float(row[1])/1e6 for row in data]

fig, ax = plt.subplots(figsize=(12, 5))
bars = ax.bar(months, revenue, color=COLORS['primary'], edgecolor='white', linewidth=0.5)

# Highlight peak months
for i, (m, r) in enumerate(zip(months, revenue)):
    if 'Dec' in m or r > 115:
        bars[i].set_color(COLORS['success'])

ax.set_xlabel('Month')
ax.set_ylabel('Revenue ($ Millions)')
ax.set_title('Monthly Revenue Trend (Aug 2024 - Jan 2026)')
ax.set_ylim(0, max(revenue) * 1.15)

# Add value labels on top
for i, (m, r) in enumerate(zip(months, revenue)):
    ax.text(i, r + 2, f'${r:.0f}M', ha='center', va='bottom', fontsize=8)

plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('reports/charts/01_monthly_revenue.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 2. QUARTERLY PERFORMANCE
# =============================================================================
print("  2. Quarterly Performance...")
data = run_sql(f"""
SELECT CONCAT(YEAR(month), '-Q', QUARTER(month)) as quarter, SUM(revenue) as revenue
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY 1 ORDER BY 1
""")

quarters = [row[0] for row in data]
qrevenue = [float(row[1])/1e6 for row in data]

fig, ax = plt.subplots(figsize=(10, 5))
colors = [COLORS['success'] if 'Q4' in q else COLORS['primary'] for q in quarters]
bars = ax.bar(quarters, qrevenue, color=colors, edgecolor='white', linewidth=0.5)

ax.set_xlabel('Quarter')
ax.set_ylabel('Revenue ($ Millions)')
ax.set_title('Quarterly Revenue Performance')
ax.set_ylim(0, max(qrevenue) * 1.15)

for i, r in enumerate(qrevenue):
    ax.text(i, r + 5, f'${r:.0f}M', ha='center', va='bottom', fontsize=10, fontweight='bold')

# Add Q4 annotation
ax.annotate('Holiday Peak', xy=(1, qrevenue[1]), xytext=(1.5, qrevenue[1] + 40),
            arrowprops=dict(arrowstyle='->', color=COLORS['accent']),
            fontsize=9, color=COLORS['accent'])

plt.tight_layout()
plt.savefig('reports/charts/02_quarterly_revenue.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 3. CATEGORY BREAKDOWN (Treemap-style horizontal bar)
# =============================================================================
print("  3. Category Performance...")
data = run_sql(f"""
SELECT category, SUM(revenue) as revenue
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY category ORDER BY revenue DESC
""")

categories = [row[0] for row in data]
cat_revenue = [float(row[1])/1e6 for row in data]

fig, ax = plt.subplots(figsize=(10, 6))
y_pos = np.arange(len(categories))
bars = ax.barh(y_pos, cat_revenue, color=COLORS['palette'][:len(categories)])

ax.set_yticks(y_pos)
ax.set_yticklabels(categories)
ax.set_xlabel('Revenue ($ Millions)')
ax.set_title('Revenue by Category')
ax.invert_yaxis()

# Add value labels
for i, r in enumerate(cat_revenue):
    pct = r / sum(cat_revenue) * 100
    ax.text(r + 2, i, f'${r:.0f}M ({pct:.1f}%)', va='center', fontsize=9)

ax.set_xlim(0, max(cat_revenue) * 1.25)
plt.tight_layout()
plt.savefig('reports/charts/03_category_revenue.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 4. MEMBERSHIP TIER DISTRIBUTION (Donut chart)
# =============================================================================
print("  4. Membership Tier Distribution...")
data = run_sql(f"""
SELECT membership_tier, COUNT(*) as cnt, SUM(lifetime_spend) as spend
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY membership_tier ORDER BY spend DESC
""")

tiers = [row[0] for row in data]
counts = [int(row[1]) for row in data]
spend = [float(row[2])/1e6 for row in data]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# Customers pie
colors_tier = [COLORS['primary'], COLORS['secondary'], COLORS['warning']]
wedges1, texts1, autotexts1 = ax1.pie(counts, labels=tiers, autopct='%1.1f%%',
                                       colors=colors_tier, startangle=90,
                                       wedgeprops=dict(width=0.6))
ax1.set_title('Customers by Tier')
centre_circle1 = plt.Circle((0, 0), 0.35, fc='white')
ax1.add_patch(centre_circle1)

# Revenue pie
wedges2, texts2, autotexts2 = ax2.pie(spend, labels=tiers, autopct='%1.1f%%',
                                       colors=colors_tier, startangle=90,
                                       wedgeprops=dict(width=0.6))
ax2.set_title('Revenue by Tier')
centre_circle2 = plt.Circle((0, 0), 0.35, fc='white')
ax2.add_patch(centre_circle2)

plt.suptitle('Membership Tier Analysis: Customers vs Revenue', fontsize=12, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig('reports/charts/04_membership_tiers.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 5. CUSTOMER ACTIVITY STATUS
# =============================================================================
print("  5. Customer Activity Status...")
data = run_sql(f"""
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
""")

statuses = [row[0].split('-')[1] for row in data]
counts = [int(row[1]) for row in data]

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
plt.savefig('reports/charts/05_customer_activity.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 6. SEASONALITY INDEX
# =============================================================================
print("  6. Seasonality Index...")
data = run_sql(f"""
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
""")

month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
indices = [float(row[1]) for row in data]

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
plt.savefig('reports/charts/06_seasonality.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 7. STORE PERFORMANCE COMPARISON
# =============================================================================
print("  7. Store Performance...")
data = run_sql(f"""
SELECT store_id, SUM(revenue) as revenue
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
GROUP BY store_id ORDER BY revenue DESC
""")

stores = [str(row[0]) for row in data]
store_rev = [float(row[1])/1e6 for row in data]
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
plt.savefig('reports/charts/07_store_performance.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 8. REVENUE CONCENTRATION (Pareto)
# =============================================================================
print("  8. Revenue Concentration...")
data = run_sql(f"""
SELECT lifetime_spend FROM {CATALOG}.{SCHEMA}.customer_golden_view ORDER BY lifetime_spend DESC
""")

spend_values = [float(row[0]) for row in data]
total = sum(spend_values)
cumsum = np.cumsum(spend_values) / total * 100
x_pct = np.arange(1, len(spend_values) + 1) / len(spend_values) * 100

fig, ax = plt.subplots(figsize=(10, 6))
ax.fill_between(x_pct, cumsum, alpha=0.3, color=COLORS['primary'])
ax.plot(x_pct, cumsum, color=COLORS['primary'], linewidth=2)

# Mark key points
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
plt.savefig('reports/charts/08_revenue_concentration.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 9. YOY COMPARISON
# =============================================================================
print("  9. Year-over-Year Comparison...")
data = run_sql(f"""
WITH monthly_data AS (
    SELECT YEAR(month) as year, MONTH(month) as month_num, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly
    GROUP BY YEAR(month), MONTH(month)
)
SELECT m2025.month_num, m2024.revenue as rev_2024, m2025.revenue as rev_2025
FROM monthly_data m2025
JOIN monthly_data m2024 ON m2025.month_num = m2024.month_num
WHERE m2025.year = 2025 AND m2024.year = 2024
ORDER BY m2025.month_num
""")

month_names_short = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec']
rev_2024 = [float(row[1])/1e6 for row in data]
rev_2025 = [float(row[2])/1e6 for row in data]

x = np.arange(len(month_names_short))
width = 0.35

fig, ax = plt.subplots(figsize=(10, 5))
bars1 = ax.bar(x - width/2, rev_2024, width, label='2024', color=COLORS['neutral'])
bars2 = ax.bar(x + width/2, rev_2025, width, label='2025', color=COLORS['primary'])

ax.set_xlabel('Month')
ax.set_ylabel('Revenue ($ Millions)')
ax.set_title('Year-over-Year Revenue Comparison')
ax.set_xticks(x)
ax.set_xticklabels(month_names_short)
ax.legend()

# Add growth labels
for i, (r24, r25) in enumerate(zip(rev_2024, rev_2025)):
    growth = (r25 - r24) / r24 * 100
    color = COLORS['success'] if growth > 0 else COLORS['accent']
    ax.text(i + width/2, r25 + 2, f'{growth:+.1f}%', ha='center', va='bottom',
            fontsize=9, color=color, fontweight='bold')

ax.set_ylim(0, max(max(rev_2024), max(rev_2025)) * 1.15)
plt.tight_layout()
plt.savefig('reports/charts/09_yoy_comparison.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 10. CUSTOMER TENURE VS LTV
# =============================================================================
print("  10. Customer Tenure vs LTV...")
data = run_sql(f"""
SELECT
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 1 THEN '< 1 yr'
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 3 THEN '1-3 yrs'
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 5 THEN '3-5 yrs'
        WHEN DATEDIFF(CURRENT_DATE(), member_since) / 365 < 10 THEN '5-10 yrs'
        ELSE '10+ yrs'
    END as tenure,
    COUNT(*) as cnt,
    AVG(lifetime_spend) as avg_ltv
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY 1
ORDER BY MIN(DATEDIFF(CURRENT_DATE(), member_since))
""")

tenures = [row[0] for row in data]
counts = [int(row[1]) for row in data]
ltvs = [float(row[2])/1000 for row in data]  # in thousands

fig, ax1 = plt.subplots(figsize=(10, 5))

x = np.arange(len(tenures))
width = 0.4

ax1.bar(x, counts, width, label='Customer Count', color=COLORS['secondary'], alpha=0.7)
ax1.set_xlabel('Customer Tenure')
ax1.set_ylabel('Number of Customers', color=COLORS['secondary'])
ax1.tick_params(axis='y', labelcolor=COLORS['secondary'])
ax1.set_xticks(x)
ax1.set_xticklabels(tenures)

ax2 = ax1.twinx()
ax2.plot(x, ltvs, 'o-', color=COLORS['accent'], linewidth=2, markersize=10, label='Avg LTV')
ax2.set_ylabel('Avg Lifetime Value ($K)', color=COLORS['accent'])
ax2.tick_params(axis='y', labelcolor=COLORS['accent'])

# Add LTV labels
for i, ltv in enumerate(ltvs):
    ax2.text(i, ltv + 2, f'${ltv:.0f}K', ha='center', va='bottom', fontsize=10, color=COLORS['accent'])

fig.suptitle('Customer Tenure vs Lifetime Value', fontsize=12, fontweight='bold')
fig.legend(loc='upper left', bbox_to_anchor=(0.12, 0.88))
plt.tight_layout()
plt.savefig('reports/charts/10_tenure_ltv.png', dpi=150, bbox_inches='tight')
plt.close()

# =============================================================================
# 11. CATEGORY GROWTH
# =============================================================================
print("  11. Category Growth...")
data = run_sql(f"""
WITH first_half AS (
    SELECT category, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly WHERE month < '2025-05-01'
    GROUP BY category
),
second_half AS (
    SELECT category, SUM(revenue) as revenue
    FROM {CATALOG}.{SCHEMA}.store_sales_monthly WHERE month >= '2025-05-01'
    GROUP BY category
)
SELECT f.category, ((s.revenue - f.revenue) / f.revenue) * 100 as growth
FROM first_half f JOIN second_half s ON f.category = s.category
ORDER BY growth DESC
""")

categories = [row[0] for row in data]
growth = [float(row[1]) for row in data]

fig, ax = plt.subplots(figsize=(10, 6))
colors_growth = [COLORS['success'] if g > 0 else COLORS['accent'] for g in growth]
y_pos = np.arange(len(categories))
bars = ax.barh(y_pos, growth, color=colors_growth)

ax.set_yticks(y_pos)
ax.set_yticklabels(categories)
ax.set_xlabel('Growth Rate (%)')
ax.set_title('Category Growth: First Half vs Second Half')
ax.axvline(x=0, color='black', linewidth=0.5)
ax.invert_yaxis()

for i, g in enumerate(growth):
    x_pos = g + 0.5 if g > 0 else g - 0.5
    ha = 'left' if g > 0 else 'right'
    ax.text(x_pos, i, f'{g:+.1f}%', va='center', ha=ha, fontsize=9)

plt.tight_layout()
plt.savefig('reports/charts/11_category_growth.png', dpi=150, bbox_inches='tight')
plt.close()

print("\nAll charts generated in reports/charts/")
print("Charts created:")
for f in sorted(os.listdir('reports/charts')):
    print(f"  - {f}")
