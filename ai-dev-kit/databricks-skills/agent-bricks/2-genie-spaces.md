# Genie Spaces

Genie Spaces are SQL-based data exploration interfaces that allow users to ask natural language questions about structured data.

## What is a Genie Space?

A Genie Space connects to Unity Catalog tables and translates natural language questions into SQL queries. The system:

1. **Understands** the table schemas and relationships
2. **Generates** SQL queries from natural language
3. **Executes** queries on a SQL warehouse
4. **Presents** results in a conversational format

## When to Use

Use a Genie Space when:
- You have structured data in Unity Catalog tables
- Users need to explore data without writing SQL
- You want to democratize data access for non-technical users

## Prerequisites

Before creating a Genie Space, you need:

1. **Tables in Unity Catalog**: Bronze, silver, or gold tables with the data
2. **SQL Warehouse**: A running warehouse to execute queries (auto-detected if not specified)

### Creating the Required Tables

Use these skills in sequence:

1. **`synthetic-data-generation`**: Generate raw parquet files in a Volume
2. **`spark-declarative-pipelines`**: Create bronze/silver/gold tables from the raw data

Example data pipeline:
```
Volume (raw parquet) → Bronze (raw tables) → Silver (cleaned) → Gold (aggregated)
```

For Genie, typically connect to **silver or gold tables** for best results.

## Creating a Genie Space

Use the `create_or_update_genie` tool:

- `display_name`: "Sales Analytics"
- `table_identifiers`: `["my_catalog.sales.customers", "my_catalog.sales.orders", "my_catalog.sales.products"]`
- `description`: "Explore sales data - customers, orders, and products"
- `sample_questions`: `["What were total sales last month?", "Who are our top 10 customers by revenue?"]`

The `warehouse_id` is optional - if not provided, the tool auto-detects the best available warehouse.

## Auto-Detection of Warehouse

When `warehouse_id` is not specified, the tool:

1. Lists all SQL warehouses in the workspace
2. Prioritizes by:
   - **Running** warehouses first (already available)
   - **Starting** warehouses second
   - **Smaller sizes** preferred (cost-efficient)
3. Returns an error if no warehouses exist

To use a specific warehouse, provide the `warehouse_id` explicitly.

## Table Selection

Choose tables carefully for best results:

| Layer | Recommended | Why |
|-------|-------------|-----|
| Bronze | No | Raw data, may have quality issues |
| Silver | Yes | Cleaned and validated |
| Gold | Yes | Aggregated, optimized for analytics |

### Tips for Table Selection

- **Include related tables**: If users ask about customers and orders, include both
- **Use descriptive column names**: `customer_name` is better than `cust_nm`
- **Add table comments**: Genie uses metadata to understand the data

## Sample Questions

Sample questions help users understand what they can ask:

Good sample questions:
- "What were total sales last month?"
- "Who are our top 10 customers by revenue?"
- "How many orders were placed in Q4?"
- "What's the average order value by region?"

These appear in the Genie UI to guide users.

## Best Practices

### Table Design for Genie

1. **Descriptive names**: Use `customer_lifetime_value` not `clv`
2. **Add comments**: `COMMENT ON TABLE sales.customers IS 'Customer master data'`
3. **Primary keys**: Define relationships clearly
4. **Date columns**: Include proper date/timestamp columns for time-based queries

### Description and Context

Provide context in the description:

```
Explore retail sales data from our e-commerce platform. Includes:
- Customers: demographics, segments, and account status
- Orders: transaction history with amounts and dates
- Products: catalog with categories and pricing

Time range: Last 6 months of data
```

### Sample Questions

Write sample questions that:
- Cover common use cases
- Demonstrate the data's capabilities
- Use natural language (not SQL terms)

## Example Workflow

1. **Generate synthetic data** using `synthetic-data-generation` skill:
   - Creates parquet files in `/Volumes/catalog/schema/raw_data/`

2. **Create tables** using `spark-declarative-pipelines` skill:
   - Creates `catalog.schema.bronze_*` → `catalog.schema.silver_*` → `catalog.schema.gold_*`

3. **Create the Genie Space**:
   - `display_name`: "My Data Explorer"
   - `table_identifiers`: `["catalog.schema.silver_customers", "catalog.schema.silver_orders"]`

4. **Add sample questions** to guide users

5. **Test** in the Databricks UI

## Updating a Genie Space

To update an existing space:

1. **Add/remove tables**: Call `create_or_update_genie` with updated `table_identifiers`
2. **Update questions**: Include new `sample_questions`
3. **Change warehouse**: Provide a different `warehouse_id`

The tool finds the existing space by name and updates it.

## Troubleshooting

### No warehouse available

- Create a SQL warehouse in the Databricks workspace
- Or provide a specific `warehouse_id`

### Queries are slow

- Ensure the warehouse is running (not stopped)
- Consider using a larger warehouse size
- Check if tables are optimized (OPTIMIZE, Z-ORDER)

### Poor query generation

- Use descriptive column names
- Add table and column comments
- Include sample questions that demonstrate the vocabulary
