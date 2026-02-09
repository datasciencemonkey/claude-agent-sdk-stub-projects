# Databricks notebook source
# MAGIC %md
# MAGIC # BJ's Wholesale Club - AI-Powered Executive Insights
# MAGIC
# MAGIC This notebook uses Claude Agent SDK to generate executive insights from the analysis data.

# COMMAND ----------

# MAGIC %pip install claude-agent-sdk nest_asyncio
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Environment Variables FIRST (before importing claude_agent_sdk)

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

# Claude configuration from environment/widgets
try:
    MODEL = dbutils.widgets.get("model")
except Exception:
    MODEL = "databricks-claude-sonnet-4-5"

try:
    DATABRICKS_HOST = dbutils.widgets.get("databricks_host")
except Exception:
    DATABRICKS_HOST = "https://fevm-serverless-9cefok.cloud.databricks.com/serving-endpoints"

try:
    DATABRICKS_TOKEN = dbutils.widgets.get("databricks_token")
except Exception:
    DATABRICKS_TOKEN = dbutils.secrets.get(scope="ai-keys", key="databricks-token")

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Model: {MODEL}")
print(f"Databricks Host: {DATABRICKS_HOST}")
print(f"Token (first 10 chars): {DATABRICKS_TOKEN[:10] if DATABRICKS_TOKEN else 'NOT SET'}...")

# COMMAND ----------

# Configure environment for Claude Agent SDK with Databricks FM API
# IMPORTANT: These must be set BEFORE importing claude_agent_sdk

os.environ["ANTHROPIC_BASE_URL"] = DATABRICKS_HOST
os.environ["ANTHROPIC_AUTH_TOKEN"] = DATABRICKS_TOKEN
os.environ["ANTHROPIC_API_KEY"] = ""  # Must be empty string for Databricks
os.environ["CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS"] = "1"
os.environ["ANTHROPIC_CUSTOM_HEADERS"] = "x-databricks-disable-beta-headers: true"
os.environ["DISABLE_PROMPT_CACHING"] = "1"

print("=" * 60)
print("Environment variables configured for Claude Agent SDK:")
print("=" * 60)
print(f"  ANTHROPIC_BASE_URL = {os.environ.get('ANTHROPIC_BASE_URL')}")
print(f"  ANTHROPIC_AUTH_TOKEN = {os.environ.get('ANTHROPIC_AUTH_TOKEN', '')[:10]}...")
print(f"  ANTHROPIC_API_KEY = '{os.environ.get('ANTHROPIC_API_KEY')}' (should be empty)")
print(f"  CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = {os.environ.get('CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS')}")
print(f"  ANTHROPIC_CUSTOM_HEADERS = {os.environ.get('ANTHROPIC_CUSTOM_HEADERS')}")
print(f"  DISABLE_PROMPT_CACHING = {os.environ.get('DISABLE_PROMPT_CACHING')}")
print("=" * 60)

# COMMAND ----------

# Now import claude_agent_sdk AFTER env vars are set
import json
import asyncio
import nest_asyncio
from claude_agent_sdk import ClaudeSDKClient, ClaudeAgentOptions

# Apply nest_asyncio to allow asyncio.run() in Databricks (which already has an event loop)
nest_asyncio.apply()

print("Claude Agent SDK imported successfully")
print("nest_asyncio applied for Databricks compatibility")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gather Analysis Data

# COMMAND ----------

# Collect all analysis data into a structured format
analysis_data = {}

# Executive summary
exec_summary = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.exec_summary").collect()
if exec_summary:
    row = exec_summary[0]
    analysis_data["executive_summary"] = {
        "total_revenue": float(row.total_revenue),
        "revenue_2025": float(row.revenue_2025),
        "revenue_2024": float(row.revenue_2024),
        "total_customers": int(row.total_customers),
        "avg_ltv": float(row.avg_ltv),
        "inner_circle_revenue_pct": float(row.inner_circle_revenue_pct),
        "active_customers": int(row.active_customers),
        "at_risk_customers": int(row.at_risk_customers)
    }

# Store performance
store_perf = spark.sql(f"""
    SELECT * FROM {CATALOG}.{SCHEMA}.store_performance_summary
    ORDER BY total_revenue DESC
""").collect()
analysis_data["top_stores"] = [
    {"store_id": row.store_id, "revenue": float(row.total_revenue), "rank": row.revenue_rank}
    for row in store_perf[:5]
]
analysis_data["bottom_stores"] = [
    {"store_id": row.store_id, "revenue": float(row.total_revenue), "rank": row.revenue_rank}
    for row in store_perf[-5:]
]

# Category performance
category_perf = spark.sql(f"""
    SELECT * FROM {CATALOG}.{SCHEMA}.category_performance_summary
    ORDER BY total_revenue DESC
""").collect()
analysis_data["categories"] = [
    {"category": row.category, "revenue": float(row.total_revenue), "share_pct": float(row.revenue_share_pct)}
    for row in category_perf
]

# Customer tiers
tier_data = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.customer_tier_summary").collect()
analysis_data["customer_tiers"] = [
    {
        "tier": row.membership_tier,
        "customers": int(row.customer_count),
        "pct_customers": float(row.pct_of_customers),
        "revenue_pct": float(row.pct_of_revenue),
        "avg_ltv": float(row.avg_lifetime_spend)
    }
    for row in tier_data
]

# Churn risk
activity_data = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.customer_activity_summary").collect()
analysis_data["customer_activity"] = [
    {
        "status": row.activity_status,
        "customers": int(row.customers),
        "pct": float(row.pct),
        "value_at_risk": float(row.total_value_at_risk)
    }
    for row in activity_data
]

# High-value at-risk
at_risk = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.high_value_at_risk_customers LIMIT 10").collect()
analysis_data["high_value_at_risk"] = [
    {
        "customer_id": row.customer_id,
        "name": f"{row.first_name} {row.last_name}",
        "tier": row.membership_tier,
        "ltv": float(row.lifetime_spend),
        "days_inactive": int(row.days_since_visit)
    }
    for row in at_risk
]

# Seasonality
seasonality = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.seasonality_index ORDER BY month_num").collect()
analysis_data["seasonality"] = [
    {"month": row.month_num, "index": float(row.seasonality_index)}
    for row in seasonality
]

print("Analysis data collected:")
print(json.dumps(analysis_data, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate AI Insights with Claude Agent SDK

# COMMAND ----------

# Create the prompt
prompt = f"""You are a senior business analyst at BJ's Wholesale Club. Based on the following data analysis,
provide an executive summary with key insights, concerns, and strategic recommendations.

Be specific with numbers and actionable in recommendations. Format your response in clear sections.

## Analysis Data

{json.dumps(analysis_data, indent=2, default=str)}

## Required Sections

1. **Executive Summary** (2-3 sentences on overall business health)

2. **Key Performance Highlights** (3-5 bullet points on what's working well)

3. **Areas of Concern** (3-5 bullet points on issues requiring attention)

4. **Immediate Action Items** (3 specific actions for next 30 days with expected impact)

5. **Strategic Recommendations** (3 longer-term initiatives for next quarter)

6. **Risk Assessment** (focus on customer churn and revenue concentration)

7. **Chart Commentary** (provide a 1-2 sentence insight for EACH of the following visualizations that will appear in the report)

Format the chart commentary section EXACTLY like this:
### Chart Commentary
**Monthly Revenue Trend:** [Your insight about revenue patterns, peaks, and trends]
**Revenue by Category:** [Your insight about category performance and concentration]
**Membership Tier Analysis:** [Your insight about tier value disparity]
**Customer Engagement Status:** [Your insight about activity levels and churn risk]
**Store Performance:** [Your insight about store variation and outliers]
**Revenue Concentration:** [Your insight about Pareto/customer concentration]
**Seasonality Index:** [Your insight about seasonal patterns and planning implications]

Be data-driven and specific. Reference actual numbers from the analysis."""

print("Generating insights with Claude Agent SDK...")

# COMMAND ----------

# Call Claude using Claude Agent SDK
async def generate_insights():
    options = ClaudeAgentOptions(
        model=MODEL,
        system_prompt="You are an expert business analyst. Provide data-driven insights. Be concise and actionable."
    )

    response_text = ""
    async with ClaudeSDKClient(options=options) as client:
        await client.query(prompt)

        async for message in client.receive_response():
            # Collect text content from the response
            if hasattr(message, 'content'):
                for block in message.content:
                    if hasattr(block, 'text'):
                        response_text += block.text
            elif hasattr(message, 'text'):
                response_text += message.text

    return response_text

# Run the async function
ai_insights = asyncio.run(generate_insights())
print(ai_insights)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save AI Insights

# COMMAND ----------

from datetime import datetime

# Save insights to a table
insights_df = spark.createDataFrame([{
    "report_date": datetime.now().strftime("%Y-%m-%d"),
    "model_used": MODEL,
    "insights": ai_insights,
    "analysis_data_json": json.dumps(analysis_data, default=str),
    "generated_at": datetime.now()
}])

insights_df.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.ai_executive_insights")

print(f"\nInsights saved to {CATALOG}.{SCHEMA}.ai_executive_insights")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Display Final Report

# COMMAND ----------

print("=" * 80)
print("BJ'S WHOLESALE CLUB - AI-POWERED EXECUTIVE INSIGHTS")
print("=" * 80)
print(f"\nGenerated: {datetime.now()}")
print(f"Model: {MODEL}")
print("=" * 80)
print(ai_insights)
print("=" * 80)
