"""Generate synthetic BJ's Club sales and customer data."""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "serverless_9cefok_catalog"
SCHEMA = "sgfs"

# Date range - last 18 months from today
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=18 * 30)  # ~18 months

# Data sizes
N_STORES = 25  # BJ's Club stores
N_CUSTOMERS = 5000

SEED = 42

# =============================================================================
# SETUP
# =============================================================================
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# =============================================================================
# CREATE INFRASTRUCTURE
# =============================================================================
print(f"Creating schema if needed...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# =============================================================================
# 1. GENERATE STORE DATA (Reference for store_id/home_club_id)
# =============================================================================
print("Generating store reference data...")

# BJ's operates primarily in East Coast US
store_regions = {
    "Northeast": ["MA", "CT", "NY", "NJ", "PA", "NH", "ME", "RI"],
    "Mid-Atlantic": ["MD", "VA", "DE", "DC"],
    "Southeast": ["NC", "SC", "GA", "FL"],
    "Midwest": ["OH", "MI"],
}

stores_data = []
store_id = 1001
for region, states in store_regions.items():
    stores_per_region = 7 if region == "Northeast" else 6 if region == "Mid-Atlantic" else 6 if region == "Southeast" else 6
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

# Weight stores by region for realistic distribution
store_weights = stores_pdf["region"].map({
    "Northeast": 0.40,
    "Mid-Atlantic": 0.25,
    "Southeast": 0.25,
    "Midwest": 0.10
})
store_weights = (store_weights / store_weights.sum()).tolist()

print(f"  Created {len(stores_pdf)} stores")

# =============================================================================
# 2. GENERATE MONTHLY STORE SALES DATA (18 months)
# =============================================================================
print("Generating monthly store sales data...")

# Product categories typical for BJ's wholesale club
categories = [
    "Grocery", "Fresh Produce", "Meat & Seafood", "Bakery",
    "Frozen Foods", "Beverages", "Household", "Electronics",
    "Health & Beauty", "Home & Garden", "Apparel", "Gas Station"
]

# Category revenue weights (some categories drive more revenue)
category_weights = {
    "Grocery": 0.20,
    "Fresh Produce": 0.08,
    "Meat & Seafood": 0.12,
    "Bakery": 0.05,
    "Frozen Foods": 0.08,
    "Beverages": 0.10,
    "Household": 0.12,
    "Electronics": 0.08,
    "Health & Beauty": 0.05,
    "Home & Garden": 0.04,
    "Apparel": 0.03,
    "Gas Station": 0.05,
}

# Generate monthly dates
months = pd.date_range(START_DATE, END_DATE, freq="MS")

sales_data = []
for month in months:
    month_str = month.strftime("%Y-%m")
    month_num = month.month

    # Seasonal multipliers
    seasonal_mult = {
        1: 0.85,   # January - post-holiday slump
        2: 0.88,
        3: 0.95,
        4: 1.00,
        5: 1.05,
        6: 1.10,   # Summer grilling season
        7: 1.15,
        8: 1.10,   # Back to school
        9: 1.00,
        10: 1.05,
        11: 1.25,  # Pre-Thanksgiving
        12: 1.40,  # Holiday season peak
    }

    for _, store in stores_pdf.iterrows():
        # Base monthly revenue varies by store size
        base_revenue = store["square_footage"] * 35  # ~$35 per sq ft per month

        # Apply seasonal adjustment
        adjusted_base = base_revenue * seasonal_mult[month_num]

        # Add some random variation per store
        store_variation = np.random.normal(1.0, 0.08)

        for category in categories:
            cat_weight = category_weights[category]

            # Category-specific seasonality
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

            # Transaction count correlates with revenue
            avg_transaction = 85 if category != "Gas Station" else 45
            transactions = int(revenue / avg_transaction * np.random.normal(1.0, 0.1))

            sales_data.append({
                "store_id": store["store_id"],
                "month": month_str,
                "category": category,
                "revenue": round(revenue, 2),
                "transaction_count": max(0, transactions),
                "units_sold": int(transactions * np.random.uniform(2.5, 4.5)),
            })

sales_pdf = pd.DataFrame(sales_data)
print(f"  Created {len(sales_pdf):,} monthly sales records")

# =============================================================================
# 3. GENERATE CUSTOMER GOLDEN VIEW DATA
# =============================================================================
print("Generating customer golden view data...")

# Membership tiers
membership_tiers = ["Inner Circle", "Club", "Business"]
tier_weights = [0.30, 0.55, 0.15]

customers_data = []
for i in range(N_CUSTOMERS):
    customer_id = f"BJ-{i+100000:07d}"

    # Assign home club (weighted by store distribution)
    home_club_id = np.random.choice(store_ids, p=store_weights)

    # Membership tier
    tier = np.random.choice(membership_tiers, p=tier_weights)

    # Tenure affects lifetime spend
    member_since = fake.date_between(
        start_date="-12y" if tier == "Inner Circle" else "-8y" if tier == "Business" else "-5y",
        end_date="-6m"
    )
    tenure_years = (END_DATE.date() - member_since).days / 365

    # Base annual spend varies by tier
    if tier == "Inner Circle":
        annual_spend_base = np.random.lognormal(8.5, 0.4)  # ~$5000 avg
    elif tier == "Business":
        annual_spend_base = np.random.lognormal(9.0, 0.5)  # ~$8000 avg
    else:
        annual_spend_base = np.random.lognormal(7.8, 0.5)  # ~$2500 avg

    # Lifetime spend = annual * tenure with some variation
    lifetime_spend = annual_spend_base * tenure_years * np.random.uniform(0.8, 1.2)

    # Category spend breakdown (proportional to category weights with variation)
    category_spend = {}
    remaining = lifetime_spend
    for cat in categories[:-1]:  # All but last
        cat_pct = category_weights[cat] * np.random.uniform(0.7, 1.4)
        cat_spend = lifetime_spend * cat_pct
        category_spend[f"spend_{cat.lower().replace(' & ', '_').replace(' ', '_')}"] = round(cat_spend, 2)
        remaining -= cat_spend
    # Last category gets remainder
    last_cat = categories[-1]
    category_spend[f"spend_{last_cat.lower().replace(' & ', '_').replace(' ', '_')}"] = round(max(0, remaining), 2)

    # Visit frequency (visits per year)
    if tier == "Inner Circle":
        visits_per_year = int(np.random.normal(48, 12))  # ~weekly
    elif tier == "Business":
        visits_per_year = int(np.random.normal(60, 15))  # More frequent
    else:
        visits_per_year = int(np.random.normal(24, 8))   # ~bi-weekly

    total_visits = int(visits_per_year * tenure_years * np.random.uniform(0.8, 1.1))

    # Average basket size
    avg_basket = lifetime_spend / max(total_visits, 1)

    # Last visit date (recent for active members)
    if np.random.random() < 0.85:  # 85% active
        last_visit = fake.date_between(start_date="-30d", end_date="today")
    else:
        last_visit = fake.date_between(start_date="-180d", end_date="-31d")

    customer_record = {
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
        **category_spend
    }
    customers_data.append(customer_record)

customers_pdf = pd.DataFrame(customers_data)
print(f"  Created {len(customers_pdf):,} customer records")

# =============================================================================
# 4. CREATE TABLES IN DATABRICKS
# =============================================================================
print(f"\nCreating tables in {CATALOG}.{SCHEMA}...")

# Convert to Spark DataFrames and save as tables
print("  Creating store_sales_monthly table...")
sales_df = spark.createDataFrame(sales_pdf)
sales_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.store_sales_monthly")

print("  Creating customer_golden_view table...")
customers_df = spark.createDataFrame(customers_pdf)
customers_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customer_golden_view")

print("\nDone!")

# =============================================================================
# 5. VALIDATION
# =============================================================================
print("\n" + "=" * 60)
print("VALIDATION")
print("=" * 60)

print(f"\n📊 store_sales_monthly:")
print(f"   Records: {len(sales_pdf):,}")
print(f"   Date range: {sales_pdf['month'].min()} to {sales_pdf['month'].max()}")
print(f"   Stores: {sales_pdf['store_id'].nunique()}")
print(f"   Categories: {sales_pdf['category'].nunique()}")
print(f"   Total revenue: ${sales_pdf['revenue'].sum():,.2f}")

print(f"\n👥 customer_golden_view:")
print(f"   Records: {len(customers_pdf):,}")
print(f"   Membership tiers: {customers_pdf['membership_tier'].value_counts().to_dict()}")
print(f"   Avg lifetime spend: ${customers_pdf['lifetime_spend'].mean():,.2f}")
print(f"   Home clubs covered: {customers_pdf['home_club_id'].nunique()}")

# Verify tables exist
print(f"\n✅ Tables created:")
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
for t in tables:
    if t.tableName in ["store_sales_monthly", "customer_golden_view"]:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{t.tableName}").collect()[0].cnt
        print(f"   {CATALOG}.{SCHEMA}.{t.tableName}: {count:,} rows")
