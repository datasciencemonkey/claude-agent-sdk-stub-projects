# Databricks notebook source
# MAGIC %md
# MAGIC # BJ's Wholesale Club - Generate Executive Report
# MAGIC
# MAGIC This notebook generates an HTML report combining AI insights with charts.

# COMMAND ----------

import os
import re
import base64
from datetime import datetime


def extract_chart_commentary(insights_text):
    """Extract chart-specific commentary from AI insights."""
    commentary = {}

    # Map chart filenames to their keywords in the AI response
    chart_keywords = [
        ("01_monthly_revenue.png", "Monthly Revenue Trend"),
        ("02_category_revenue.png", "Revenue by Category"),
        ("03_membership_tiers.png", "Membership Tier"),
        ("04_customer_activity.png", "Customer Engagement"),
        ("05_store_performance.png", "Store Performance"),
        ("06_revenue_concentration.png", "Revenue Concentration"),
        ("07_seasonality.png", "Seasonality"),
    ]

    # Look for Chart Commentary section
    if "Chart Commentary" in insights_text:
        section = insights_text.split("Chart Commentary", 1)[1]

        for filename, keyword in chart_keywords:
            # Find the keyword and extract text until next ** or end of section
            pattern = rf"\*\*{re.escape(keyword)}[^:]*:\*\*\s*(.+?)(?=\*\*[A-Z]|\Z)"
            match = re.search(pattern, section, re.DOTALL | re.IGNORECASE)
            if match:
                text = match.group(1).strip()
                # Clean up - remove extra newlines and trim
                text = re.sub(r'\n+', ' ', text).strip()
                commentary[filename] = text

    return commentary

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

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/reports"
print(f"Reading charts from: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load AI Insights

# COMMAND ----------

# Get the latest AI insights
insights_df = spark.sql(f"""
    SELECT insights, model_used, generated_at
    FROM {CATALOG}.{SCHEMA}.ai_executive_insights
    ORDER BY generated_at DESC
    LIMIT 1
""").collect()

if insights_df:
    ai_insights = insights_df[0].insights
    model_used = insights_df[0].model_used
    generated_at = insights_df[0].generated_at
    print(f"Loaded insights from {generated_at}")
    print(f"Model: {model_used}")
else:
    ai_insights = "No AI insights available."
    model_used = "N/A"
    generated_at = datetime.now()
    print("WARNING: No AI insights found!")

# Extract chart commentary from AI insights
chart_commentary = extract_chart_commentary(ai_insights)
print(f"Extracted commentary for {len(chart_commentary)} charts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Key Metrics

# COMMAND ----------

# Load executive summary
exec_summary = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.exec_summary").collect()[0]

metrics = {
    "total_revenue": float(exec_summary.total_revenue),
    "revenue_2025": float(exec_summary.revenue_2025),
    "revenue_2024": float(exec_summary.revenue_2024),
    "total_customers": int(exec_summary.total_customers),
    "avg_ltv": float(exec_summary.avg_ltv),
    "inner_circle_pct": float(exec_summary.inner_circle_revenue_pct),
    "active_customers": int(exec_summary.active_customers),
    "at_risk_customers": int(exec_summary.at_risk_customers)
}

yoy_growth = ((metrics["revenue_2025"] - metrics["revenue_2024"]) / metrics["revenue_2024"]) * 100

print(f"Total Revenue: ${metrics['total_revenue']/1e6:.1f}M")
print(f"YoY Growth: {yoy_growth:.1f}%")
print(f"Total Customers: {metrics['total_customers']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate HTML Report

# COMMAND ----------

# Chart files to include in report
chart_files = [
    ("01_monthly_revenue.png", "Monthly Revenue Trend"),
    ("02_category_revenue.png", "Revenue by Category"),
    ("03_membership_tiers.png", "Membership Tier Analysis"),
    ("04_customer_activity.png", "Customer Engagement Status"),
    ("05_store_performance.png", "Store Performance"),
    ("06_revenue_concentration.png", "Revenue Concentration"),
    ("07_seasonality.png", "Seasonality Index")
]

# COMMAND ----------

# Generate HTML report
html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>BJ's Wholesale Club - Executive Report</title>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
        .header {{ background: linear-gradient(135deg, #1E3A5F 0%, #3498db 100%); color: white; padding: 40px; text-align: center; border-radius: 10px; margin-bottom: 30px; }}
        .header h1 {{ margin: 0; font-size: 2.5em; }}
        .header p {{ margin: 10px 0 0; opacity: 0.9; }}
        .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-bottom: 30px; }}
        .metric {{ background: white; padding: 25px; border-radius: 10px; text-align: center; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .metric .value {{ font-size: 2em; font-weight: bold; color: #27ae60; }}
        .metric .label {{ color: #666; margin-top: 5px; }}
        .section {{ background: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .section h2 {{ color: #1E3A5F; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        .insights {{ line-height: 1.8; }}
        .chart {{ text-align: center; margin: 30px 0; padding-bottom: 20px; border-bottom: 1px solid #eee; }}
        .chart img {{ max-width: 100%; border-radius: 5px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .chart-commentary {{ color: #555; font-style: italic; margin: 15px auto 0; text-align: center; max-width: 700px; line-height: 1.6; padding: 12px 20px; background: #f8f9fa; border-radius: 5px; border-left: 3px solid #3498db; }}
        .footer {{ text-align: center; color: #666; padding: 20px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>BJ's Wholesale Club</h1>
        <p>Executive Analytics Report</p>
        <p>Generated: {datetime.now().strftime('%B %d, %Y')} | Model: {model_used}</p>
    </div>

    <div class="metrics">
        <div class="metric">
            <div class="value">${metrics['total_revenue']/1e6:.1f}M</div>
            <div class="label">Total Revenue</div>
        </div>
        <div class="metric">
            <div class="value">{yoy_growth:+.1f}%</div>
            <div class="label">YoY Growth</div>
        </div>
        <div class="metric">
            <div class="value">{metrics['total_customers']:,}</div>
            <div class="label">Total Customers</div>
        </div>
        <div class="metric">
            <div class="value">${metrics['avg_ltv']/1000:.1f}K</div>
            <div class="label">Avg Lifetime Value</div>
        </div>
    </div>

    <div class="metrics" style="grid-template-columns: repeat(3, 1fr);">
        <div class="metric">
            <div class="value">{metrics['inner_circle_pct']:.1f}%</div>
            <div class="label">Inner Circle Revenue %</div>
        </div>
        <div class="metric">
            <div class="value">{metrics['active_customers']:,}</div>
            <div class="label">Active Customers (30d)</div>
        </div>
        <div class="metric">
            <div class="value" style="color: #e74c3c;">{metrics['at_risk_customers']:,}</div>
            <div class="label">At-Risk Customers</div>
        </div>
    </div>

    <div class="section">
        <h2>AI-Generated Executive Insights</h2>
        <div class="insights">
            {ai_insights.replace(chr(10), '<br>')}
        </div>
    </div>

    <div class="section">
        <h2>Visual Analytics</h2>
"""

# Add charts with base64 encoding and AI commentary
for chart_file, chart_title in chart_files:
    chart_path = f"{VOLUME_PATH}/{chart_file}"
    commentary = chart_commentary.get(chart_file, "")
    try:
        # Read chart as binary and encode to base64
        with open(chart_path, "rb") as f:
            img_data = base64.b64encode(f.read()).decode()
        commentary_html = f'<p class="chart-commentary">{commentary}</p>' if commentary else ''
        html_content += f"""
        <div class="chart">
            <h3>{chart_title}</h3>
            <img src="data:image/png;base64,{img_data}" alt="{chart_title}">
            {commentary_html}
        </div>
        """
    except Exception as e:
        print(f"Warning: Could not embed {chart_file}: {e}")
        html_content += f'<p>{chart_title} (chart not available)</p>'

html_content += """
    </div>

    <div class="footer">
        <p>Generated by BJ's Analytics Pipeline | Powered by Claude AI</p>
    </div>
</body>
</html>
"""

html_path = f"{VOLUME_PATH}/BJs_Executive_Report.html"
with open(html_path, "w") as f:
    f.write(html_content)
print(f"HTML Report generated: {html_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary

# COMMAND ----------

print("=" * 60)
print("REPORT GENERATION COMPLETE")
print("=" * 60)
print(f"\nReport saved to: {VOLUME_PATH}")
print("\nGenerated file:")
print(f"  - BJs_Executive_Report.html")

# List all files
files = dbutils.fs.ls(VOLUME_PATH)
print(f"\nAll files in volume ({len(files)}):")
for f in files:
    print(f"  - {f.name} ({f.size/1024:.1f} KB)")

print(f"\nTo download the report:")
print(f"  HTML: {VOLUME_PATH}/BJs_Executive_Report.html")
