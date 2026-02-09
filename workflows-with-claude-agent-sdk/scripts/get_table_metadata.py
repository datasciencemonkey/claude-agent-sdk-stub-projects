"""Fetch table metadata from Databricks."""
import os
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
    return response

# Get metadata for store_sales_monthly
print("=" * 70)
print("TABLE: store_sales_monthly")
print("=" * 70)

desc = run_sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{SCHEMA}.store_sales_monthly")
if desc.result and desc.result.data_array:
    for row in desc.result.data_array:
        print(f"{row[0]:<30} {row[1]:<20} {row[2] or ''}")

print("\n")

# Get metadata for customer_golden_view
print("=" * 70)
print("TABLE: customer_golden_view")
print("=" * 70)

desc2 = run_sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{SCHEMA}.customer_golden_view")
if desc2.result and desc2.result.data_array:
    for row in desc2.result.data_array:
        print(f"{row[0]:<30} {row[1]:<20} {row[2] or ''}")

# Also get sample statistics
print("\n")
print("=" * 70)
print("DATA STATISTICS")
print("=" * 70)

stats1 = run_sql(f"""
SELECT
    MIN(month) as min_month,
    MAX(month) as max_month,
    COUNT(DISTINCT store_id) as num_stores,
    COUNT(DISTINCT category) as num_categories,
    SUM(revenue) as total_revenue,
    SUM(transaction_count) as total_transactions
FROM {CATALOG}.{SCHEMA}.store_sales_monthly
""")
if stats1.result and stats1.result.data_array:
    row = stats1.result.data_array[0]
    print(f"\nstore_sales_monthly stats:")
    print(f"  Date range: {row[0]} to {row[1]}")
    print(f"  Stores: {row[2]}")
    print(f"  Categories: {row[3]}")
    print(f"  Total Revenue: ${float(row[4]):,.2f}")
    print(f"  Total Transactions: {int(row[5]):,}")

stats2 = run_sql(f"""
SELECT
    COUNT(*) as total_customers,
    COUNT(DISTINCT home_club_id) as num_home_clubs,
    COUNT(DISTINCT membership_tier) as num_tiers,
    AVG(lifetime_spend) as avg_lifetime_spend,
    MIN(member_since) as earliest_member,
    MAX(last_visit_date) as latest_visit
FROM {CATALOG}.{SCHEMA}.customer_golden_view
""")
if stats2.result and stats2.result.data_array:
    row = stats2.result.data_array[0]
    print(f"\ncustomer_golden_view stats:")
    print(f"  Total Customers: {int(row[0]):,}")
    print(f"  Home Clubs: {row[1]}")
    print(f"  Membership Tiers: {row[2]}")
    print(f"  Avg Lifetime Spend: ${float(row[3]):,.2f}")
    print(f"  Earliest Member: {row[4]}")
    print(f"  Latest Visit: {row[5]}")
