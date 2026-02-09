"""Generate synthetic BJ's Club sales and customer data, upload to Databricks."""
import os
import io
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# =============================================================================
# CONFIGURATION
# =============================================================================
load_dotenv()

CATALOG = os.getenv("CATALOG_NAME")
SCHEMA = os.getenv("SCHEMA_NAME")
DATABRICKS_HOST = os.getenv("DATABRICKS_BASE_URL")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

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

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")

# Initialize Databricks client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
print(f"Connected to: {w.config.host}")

# =============================================================================
# 1. GENERATE STORE DATA (Reference for store_id/home_club_id)
# =============================================================================
print("\n1. Generating store reference data...")

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

# Weight stores by region for realistic distribution
store_weights = stores_pdf["region"].map({
    "Northeast": 0.40,
    "Mid-Atlantic": 0.25,
    "Southeast": 0.25,
    "Midwest": 0.10
})
store_weights = (store_weights / store_weights.sum()).tolist()

print(f"   Created {len(stores_pdf)} stores")

# =============================================================================
# 2. GENERATE MONTHLY STORE SALES DATA (18 months)
# =============================================================================
print("\n2. Generating monthly store sales data...")

# Product categories typical for BJ's wholesale club
categories = [
    "Grocery", "Fresh Produce", "Meat & Seafood", "Bakery",
    "Frozen Foods", "Beverages", "Household", "Electronics",
    "Health & Beauty", "Home & Garden", "Apparel", "Gas Station"
]

# Category revenue weights
category_weights = {
    "Grocery": 0.20, "Fresh Produce": 0.08, "Meat & Seafood": 0.12,
    "Bakery": 0.05, "Frozen Foods": 0.08, "Beverages": 0.10,
    "Household": 0.12, "Electronics": 0.08, "Health & Beauty": 0.05,
    "Home & Garden": 0.04, "Apparel": 0.03, "Gas Station": 0.05,
}

# Generate monthly dates
months = pd.date_range(START_DATE, END_DATE, freq="MS")

sales_data = []
for month in months:
    month_str = month.strftime("%Y-%m")
    month_num = month.month

    # Seasonal multipliers
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
                "month": month_str,
                "category": category,
                "revenue": round(revenue, 2),
                "transaction_count": max(0, transactions),
                "units_sold": int(transactions * np.random.uniform(2.5, 4.5)),
            })

sales_pdf = pd.DataFrame(sales_data)
print(f"   Created {len(sales_pdf):,} monthly sales records")

# =============================================================================
# 3. GENERATE CUSTOMER GOLDEN VIEW DATA
# =============================================================================
print("\n3. Generating customer golden view data...")

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
        "member_since": str(member_since),
        "lifetime_spend": round(lifetime_spend, 2),
        "total_visits": max(1, total_visits),
        "avg_basket_size": round(avg_basket, 2),
        "last_visit_date": str(last_visit),
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
print(f"   Created {len(customers_pdf):,} customer records")

# =============================================================================
# 4. SAVE CSVs LOCALLY
# =============================================================================
print("\n4. Saving CSV files locally...")
os.makedirs("data", exist_ok=True)

sales_pdf.to_csv("data/store_sales_monthly.csv", index=False)
customers_pdf.to_csv("data/customer_golden_view.csv", index=False)
print("   Saved data/store_sales_monthly.csv")
print("   Saved data/customer_golden_view.csv")

# =============================================================================
# 5. CREATE VOLUME AND UPLOAD FILES
# =============================================================================
print(f"\n5. Creating volume and uploading to Databricks...")

# Create volume if not exists
try:
    w.volumes.read(f"{CATALOG}.{SCHEMA}.raw_data")
    print(f"   Volume {CATALOG}.{SCHEMA}.raw_data already exists")
except Exception:
    print(f"   Creating volume {CATALOG}.{SCHEMA}.raw_data...")
    from databricks.sdk.service.catalog import VolumeType
    w.volumes.create(
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        name="raw_data",
        volume_type=VolumeType.MANAGED
    )
    print(f"   Volume created")

# Upload files
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

print(f"   Uploading store_sales_monthly.csv...")
with open("data/store_sales_monthly.csv", "rb") as f:
    w.files.upload(f"{volume_path}/store_sales_monthly.csv", f, overwrite=True)

print(f"   Uploading customer_golden_view.csv...")
with open("data/customer_golden_view.csv", "rb") as f:
    w.files.upload(f"{volume_path}/customer_golden_view.csv", f, overwrite=True)

print("   Files uploaded successfully!")

# =============================================================================
# 6. CREATE TABLES FROM CSV FILES
# =============================================================================
print(f"\n6. Creating tables in {CATALOG}.{SCHEMA}...")

# Find a serverless warehouse
warehouses = list(w.warehouses.list())
serverless_wh = None
for wh in warehouses:
    if wh.enable_serverless_compute and wh.state.value == "RUNNING":
        serverless_wh = wh
        break
    elif wh.enable_serverless_compute:
        serverless_wh = wh

if not serverless_wh:
    # Fall back to any warehouse
    for wh in warehouses:
        if wh.state.value == "RUNNING":
            serverless_wh = wh
            break
    if not serverless_wh and warehouses:
        serverless_wh = warehouses[0]

if not serverless_wh:
    print("   ERROR: No SQL warehouse found!")
    exit(1)

print(f"   Using warehouse: {serverless_wh.name} (ID: {serverless_wh.id})")

# Start warehouse if not running
if serverless_wh.state.value != "RUNNING":
    print(f"   Starting warehouse...")
    w.warehouses.start(serverless_wh.id)
    import time
    for _ in range(60):
        wh = w.warehouses.get(serverless_wh.id)
        if wh.state.value == "RUNNING":
            break
        time.sleep(5)
    print(f"   Warehouse started")

def execute_sql(sql, description=""):
    """Execute SQL using statement execution API."""
    if description:
        print(f"   {description}...")
    response = w.statement_execution.execute_statement(
        warehouse_id=serverless_wh.id,
        statement=sql,
        wait_timeout="50s"
    )
    # Poll for completion if still running
    import time
    while response.status.state.value in ["PENDING", "RUNNING"]:
        time.sleep(2)
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status.state.value == "FAILED":
        print(f"   ERROR: {response.status.error}")
        return None
    return response

# Create store_sales_monthly table using CREATE TABLE AS SELECT from read_files
execute_sql(f"""
DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.store_sales_monthly
""", "Dropping existing store_sales_monthly table")

execute_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.store_sales_monthly AS
SELECT
    CAST(store_id AS INT) as store_id,
    month,
    category,
    CAST(revenue AS DOUBLE) as revenue,
    CAST(transaction_count AS INT) as transaction_count,
    CAST(units_sold AS INT) as units_sold
FROM read_files('{volume_path}/store_sales_monthly.csv',
    format => 'csv',
    header => true
)
""", "Creating store_sales_monthly table from CSV")

# Create customer_golden_view table
execute_sql(f"""
DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.customer_golden_view
""", "Dropping existing customer_golden_view table")

execute_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.customer_golden_view AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    CAST(home_club_id AS INT) as home_club_id,
    membership_tier,
    CAST(member_since AS DATE) as member_since,
    CAST(lifetime_spend AS DOUBLE) as lifetime_spend,
    CAST(total_visits AS INT) as total_visits,
    CAST(avg_basket_size AS DOUBLE) as avg_basket_size,
    CAST(last_visit_date AS DATE) as last_visit_date,
    CAST(spend_grocery AS DOUBLE) as spend_grocery,
    CAST(spend_fresh_produce AS DOUBLE) as spend_fresh_produce,
    CAST(spend_meat_seafood AS DOUBLE) as spend_meat_seafood,
    CAST(spend_bakery AS DOUBLE) as spend_bakery,
    CAST(spend_frozen_foods AS DOUBLE) as spend_frozen_foods,
    CAST(spend_beverages AS DOUBLE) as spend_beverages,
    CAST(spend_household AS DOUBLE) as spend_household,
    CAST(spend_electronics AS DOUBLE) as spend_electronics,
    CAST(spend_health_beauty AS DOUBLE) as spend_health_beauty,
    CAST(spend_home_garden AS DOUBLE) as spend_home_garden,
    CAST(spend_apparel AS DOUBLE) as spend_apparel,
    CAST(spend_gas_station AS DOUBLE) as spend_gas_station
FROM read_files('{volume_path}/customer_golden_view.csv',
    format => 'csv',
    header => true
)
""", "Creating customer_golden_view table from CSV")

# =============================================================================
# 7. VALIDATION
# =============================================================================
print(f"\n{'='*60}")
print("VALIDATION")
print('='*60)

# Count rows in tables
sales_count = execute_sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.store_sales_monthly")
if sales_count and sales_count.result and sales_count.result.data_array:
    cnt = sales_count.result.data_array[0][0]
    print(f"\n   store_sales_monthly: {cnt} rows")
else:
    print(f"\n   store_sales_monthly: Unable to count rows")

customer_count = execute_sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.customer_golden_view")
if customer_count and customer_count.result and customer_count.result.data_array:
    cnt = customer_count.result.data_array[0][0]
    print(f"   customer_golden_view: {cnt} rows")
else:
    print(f"   customer_golden_view: Unable to count rows")

# Show sample data
print(f"\n   Sample from store_sales_monthly:")
sample_sales = execute_sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.store_sales_monthly LIMIT 3")
if sample_sales and sample_sales.result and sample_sales.result.data_array:
    for row in sample_sales.result.data_array:
        print(f"      {row}")
else:
    print(f"      No data available")

print(f"\n   Sample from customer_golden_view:")
sample_customers = execute_sql(f"SELECT customer_id, first_name, last_name, home_club_id, membership_tier, lifetime_spend FROM {CATALOG}.{SCHEMA}.customer_golden_view LIMIT 3")
if sample_customers and sample_customers.result and sample_customers.result.data_array:
    for row in sample_customers.result.data_array:
        print(f"      {row}")
else:
    print(f"      No data available")

print(f"\n{'='*60}")
print("COMPLETE!")
print(f"Tables created:")
print(f"  - {CATALOG}.{SCHEMA}.store_sales_monthly")
print(f"  - {CATALOG}.{SCHEMA}.customer_golden_view")
print('='*60)
