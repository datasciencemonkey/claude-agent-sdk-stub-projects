# Databricks notebook source
# MAGIC %md
# MAGIC # BJ's Wholesale Club - Data Generation & Analysis Pipeline
# MAGIC
# MAGIC This job generates synthetic sales and customer data for BJ's Wholesale Club,
# MAGIC creates tables in Unity Catalog, and can optionally generate an executive report.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `store_sales_monthly` - 18 months of sales by store and category
# MAGIC - `customer_golden_view` - Customer master with lifetime value and spend by category

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters from job or use defaults
try:
    CATALOG = dbutils.widgets.get("catalog_name")
except:
    CATALOG = "serverless_9cefok_catalog"

try:
    SCHEMA = dbutils.widgets.get("schema_name")
except:
    SCHEMA = "sgfs"

# Data configuration
N_STORES = 25
N_CUSTOMERS = 5000
SEED = 42

# Date range - last 18 months from today
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=18 * 30)

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Date Range: {START_DATE.date()} to {END_DATE.date()}")
print(f"  Stores: {N_STORES}")
print(f"  Customers: {N_CUSTOMERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# Create schema if needed
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Store Reference Data

# COMMAND ----------

print("Generating store reference data...")

store_regions = {
    "Northeast": ["MA", "CT", "NY", "NJ", "PA", "NH", "ME", "RI"],
    "Mid-Atlantic": ["MD", "VA", "DE", "DC"],
    "Southeast": ["NC", "SC", "GA", "FL"],
    "Midwest": ["OH", "MI"],
}

stores_data = []
store_id = 1001
for region, states in store_regions.items():
    stores_per_region = 7 if region == "Northeast" else 6
    for i in range(min(stores_per_region, N_STORES - len(stores_data))):
        state = np.random.choice(states)
        stores_data.append({
            "store_id": store_id,
            "store_name": f"BJ's Club #{store_id}",
            "city": fake.city(),
            "state": state,
            "region": region,
            "opened_date": fake.date_between(start_date="-15y", end_date="-2y"),
            "square_footage": int(np.random.normal(110000, 15000)),
        })
        store_id += 1
        if len(stores_data) >= N_STORES:
            break
    if len(stores_data) >= N_STORES:
        break

stores_pdf = pd.DataFrame(stores_data)
store_ids = stores_pdf["store_id"].tolist()

# Weight stores by region
store_weights = stores_pdf["region"].map({
    "Northeast": 0.40, "Mid-Atlantic": 0.25, "Southeast": 0.25, "Midwest": 0.10
})
store_weights = (store_weights / store_weights.sum()).tolist()

print(f"  Created {len(stores_pdf)} stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Monthly Store Sales Data

# COMMAND ----------

print("Generating monthly store sales data...")

categories = [
    "Grocery", "Fresh Produce", "Meat & Seafood", "Bakery",
    "Frozen Foods", "Beverages", "Household", "Electronics",
    "Health & Beauty", "Home & Garden", "Apparel", "Gas Station"
]

category_weights = {
    "Grocery": 0.20, "Fresh Produce": 0.08, "Meat & Seafood": 0.12,
    "Bakery": 0.05, "Frozen Foods": 0.08, "Beverages": 0.10,
    "Household": 0.12, "Electronics": 0.08, "Health & Beauty": 0.05,
    "Home & Garden": 0.04, "Apparel": 0.03, "Gas Station": 0.05,
}

months = pd.date_range(START_DATE, END_DATE, freq="MS")

sales_data = []
for month in months:
    month_str = month.strftime("%Y-%m")
    month_num = month.month

    seasonal_mult = {
        1: 0.85, 2: 0.88, 3: 0.95, 4: 1.00, 5: 1.05, 6: 1.10,
        7: 1.15, 8: 1.10, 9: 1.00, 10: 1.05, 11: 1.25, 12: 1.40,
    }

    for _, store in stores_pdf.iterrows():
        base_revenue = store["square_footage"] * 35
        adjusted_base = base_revenue * seasonal_mult[month_num]
        store_variation = np.random.normal(1.0, 0.08)

        for category in categories:
            cat_weight = category_weights[category]

            if category == "Gas Station":
                cat_seasonal = 1.15 if month_num in [6, 7, 8] else 0.95
            elif category in ["Fresh Produce", "Meat & Seafood"]:
                cat_seasonal = 1.20 if month_num in [5, 6, 7, 8] else 0.90
            elif category == "Electronics":
                cat_seasonal = 1.50 if month_num in [11, 12] else 0.85
            elif category == "Home & Garden":
                cat_seasonal = 1.30 if month_num in [4, 5, 6] else 0.80
            else:
                cat_seasonal = 1.0

            revenue = adjusted_base * cat_weight * store_variation * cat_seasonal
            revenue = max(0, revenue * np.random.normal(1.0, 0.05))

            avg_transaction = 85 if category != "Gas Station" else 45
            transactions = int(revenue / avg_transaction * np.random.normal(1.0, 0.1))

            sales_data.append({
                "store_id": store["store_id"],
                "month": month.date(),
                "category": category,
                "revenue": round(revenue, 2),
                "transaction_count": max(0, transactions),
                "units_sold": int(transactions * np.random.uniform(2.5, 4.5)),
            })

sales_pdf = pd.DataFrame(sales_data)
print(f"  Created {len(sales_pdf):,} monthly sales records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Customer Golden View Data

# COMMAND ----------

print("Generating customer golden view data...")

membership_tiers = ["Inner Circle", "Club", "Business"]
tier_weights_list = [0.30, 0.55, 0.15]

customers_data = []
for i in range(N_CUSTOMERS):
    customer_id = f"BJ-{i+100000:07d}"
    home_club_id = np.random.choice(store_ids, p=store_weights)
    tier = np.random.choice(membership_tiers, p=tier_weights_list)

    member_since = fake.date_between(
        start_date="-12y" if tier == "Inner Circle" else "-8y" if tier == "Business" else "-5y",
        end_date="-6m"
    )
    tenure_years = (END_DATE.date() - member_since).days / 365

    if tier == "Inner Circle":
        annual_spend_base = np.random.lognormal(8.5, 0.4)
    elif tier == "Business":
        annual_spend_base = np.random.lognormal(9.0, 0.5)
    else:
        annual_spend_base = np.random.lognormal(7.8, 0.5)

    lifetime_spend = annual_spend_base * tenure_years * np.random.uniform(0.8, 1.2)

    # Category spend breakdown
    spend_grocery = round(lifetime_spend * 0.20 * np.random.uniform(0.7, 1.4), 2)
    spend_fresh_produce = round(lifetime_spend * 0.08 * np.random.uniform(0.7, 1.4), 2)
    spend_meat_seafood = round(lifetime_spend * 0.12 * np.random.uniform(0.7, 1.4), 2)
    spend_bakery = round(lifetime_spend * 0.05 * np.random.uniform(0.7, 1.4), 2)
    spend_frozen_foods = round(lifetime_spend * 0.08 * np.random.uniform(0.7, 1.4), 2)
    spend_beverages = round(lifetime_spend * 0.10 * np.random.uniform(0.7, 1.4), 2)
    spend_household = round(lifetime_spend * 0.12 * np.random.uniform(0.7, 1.4), 2)
    spend_electronics = round(lifetime_spend * 0.08 * np.random.uniform(0.7, 1.4), 2)
    spend_health_beauty = round(lifetime_spend * 0.05 * np.random.uniform(0.7, 1.4), 2)
    spend_home_garden = round(lifetime_spend * 0.04 * np.random.uniform(0.7, 1.4), 2)
    spend_apparel = round(lifetime_spend * 0.03 * np.random.uniform(0.7, 1.4), 2)
    spend_gas_station = round(lifetime_spend * 0.05 * np.random.uniform(0.7, 1.4), 2)

    if tier == "Inner Circle":
        visits_per_year = int(np.random.normal(48, 12))
    elif tier == "Business":
        visits_per_year = int(np.random.normal(60, 15))
    else:
        visits_per_year = int(np.random.normal(24, 8))

    total_visits = int(visits_per_year * tenure_years * np.random.uniform(0.8, 1.1))
    avg_basket = lifetime_spend / max(total_visits, 1)

    if np.random.random() < 0.85:
        last_visit = fake.date_between(start_date="-30d", end_date="today")
    else:
        last_visit = fake.date_between(start_date="-180d", end_date="-31d")

    customers_data.append({
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "home_club_id": home_club_id,
        "membership_tier": tier,
        "member_since": member_since,
        "lifetime_spend": round(lifetime_spend, 2),
        "total_visits": max(1, total_visits),
        "avg_basket_size": round(avg_basket, 2),
        "last_visit_date": last_visit,
        "spend_grocery": spend_grocery,
        "spend_fresh_produce": spend_fresh_produce,
        "spend_meat_seafood": spend_meat_seafood,
        "spend_bakery": spend_bakery,
        "spend_frozen_foods": spend_frozen_foods,
        "spend_beverages": spend_beverages,
        "spend_household": spend_household,
        "spend_electronics": spend_electronics,
        "spend_health_beauty": spend_health_beauty,
        "spend_home_garden": spend_home_garden,
        "spend_apparel": spend_apparel,
        "spend_gas_station": spend_gas_station,
    })

customers_pdf = pd.DataFrame(customers_data)
print(f"  Created {len(customers_pdf):,} customer records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Tables in Unity Catalog

# COMMAND ----------

print(f"Creating tables in {CATALOG}.{SCHEMA}...")

# Drop existing tables to avoid schema merge issues
print("  Dropping existing tables if they exist...")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.store_sales_monthly")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.customer_golden_view")

# Create store_sales_monthly table
print("  Creating store_sales_monthly...")
sales_df = spark.createDataFrame(sales_pdf)
sales_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.store_sales_monthly")

# Create customer_golden_view table
print("  Creating customer_golden_view...")
customers_df = spark.createDataFrame(customers_pdf)
customers_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.customer_golden_view")

print("Tables created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation

# COMMAND ----------

print("=" * 60)
print("VALIDATION")
print("=" * 60)

# Count rows
sales_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.store_sales_monthly").collect()[0][0]
customer_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.customer_golden_view").collect()[0][0]

print(f"\nstore_sales_monthly: {sales_count:,} rows")
print(f"customer_golden_view: {customer_count:,} rows")

# Summary stats
print("\nSales Summary:")
spark.sql(f"""
SELECT
    MIN(month) as min_month,
    MAX(month) as max_month,
    COUNT(DISTINCT store_id) as stores,
    COUNT(DISTINCT category) as categories,
    ROUND(SUM(revenue), 2) as total_revenue
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
""").show()

print("Customer Summary:")
spark.sql(f"""
SELECT
    membership_tier,
    COUNT(*) as customers,
    ROUND(AVG(lifetime_spend), 2) as avg_ltv,
    ROUND(SUM(lifetime_spend), 2) as total_ltv
FROM {CATALOG}.{SCHEMA}.customer_golden_view
GROUP BY membership_tier
ORDER BY total_ltv DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline Complete

# COMMAND ----------

print("=" * 60)
print("PIPELINE COMPLETE")
print("=" * 60)
print(f"""
Tables Created:
  - {CATALOG}.{SCHEMA}.store_sales_monthly ({sales_count:,} rows)
  - {CATALOG}.{SCHEMA}.customer_golden_view ({customer_count:,} rows)

Data Summary:
  - {N_STORES} stores across 4 regions
  - 18 months of sales data
  - 12 product categories
  - {N_CUSTOMERS:,} customers with lifetime value data

Run completed at: {datetime.now()}
""")
