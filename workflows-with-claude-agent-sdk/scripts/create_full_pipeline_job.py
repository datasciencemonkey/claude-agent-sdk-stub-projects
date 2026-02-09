"""Create complete multi-task Databricks Job for BJ's Data Pipeline."""
import os
import base64
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, Source, TaskDependency,
    JobParameterDefinition
)
from databricks.sdk.service.workspace import ImportFormat, Language

load_dotenv()

# Configuration from .env
DATABRICKS_HOST = os.getenv("DATABRICKS_BASE_URL", "").replace("/serving-endpoints", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG_NAME = os.getenv("CATALOG_NAME")
SCHEMA_NAME = os.getenv("SCHEMA_NAME")

# Claude/AI configuration
MODEL = os.getenv("MODEL", "databricks-claude-sonnet-4-5")
DATABRICKS_SERVING_ENDPOINT = os.getenv("DATABRICKS_BASE_URL", "").rstrip("/")
if "/serving-endpoints" not in DATABRICKS_SERVING_ENDPOINT:
    DATABRICKS_SERVING_ENDPOINT = DATABRICKS_SERVING_ENDPOINT + "/serving-endpoints"

# Environment variables for Claude
CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = os.getenv("CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS", "1")
ANTHROPIC_CUSTOM_HEADERS = os.getenv("ANTHROPIC_CUSTOM_HEADERS", "x-databricks-disable-beta-headers: true")
DISABLE_PROMPT_CACHING = os.getenv("DISABLE_PROMPT_CACHING", "1")

print("=" * 70)
print("Creating Multi-Task Databricks Job: BJ's Complete Pipeline")
print("=" * 70)
print(f"Host: {DATABRICKS_HOST}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")
print(f"Claude Model: {MODEL}")

# Initialize client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Get current user
current_user = w.current_user.me()
username = current_user.user_name
print(f"User: {username}")

# Workspace paths
workspace_path = f"/Workspace/Users/{username}/jobs"

# Notebook paths - all 5 notebooks in the pipeline
notebooks = {
    "data_pipeline": {
        "local": "jobs/bjs_data_pipeline.py",
        "remote": f"{workspace_path}/bjs_data_pipeline"
    },
    "executive_analysis": {
        "local": "jobs/bjs_executive_analysis.py",
        "remote": f"{workspace_path}/bjs_executive_analysis"
    },
    "ai_insights": {
        "local": "jobs/bjs_ai_insights.py",
        "remote": f"{workspace_path}/bjs_ai_insights"
    },
    "generate_charts": {
        "local": "jobs/bjs_generate_charts.py",
        "remote": f"{workspace_path}/bjs_generate_charts"
    },
    "generate_report": {
        "local": "jobs/bjs_generate_report.py",
        "remote": f"{workspace_path}/bjs_generate_report"
    }
}

# Upload all notebooks
print(f"\nUploading notebooks to {workspace_path}...")
try:
    w.workspace.mkdirs(workspace_path)
except Exception:
    pass

for name, paths in notebooks.items():
    print(f"  Uploading {name}...")
    with open(paths["local"], "r") as f:
        content = f.read()
    w.workspace.import_(
        path=paths["remote"],
        content=base64.b64encode(content.encode()).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
print("  All notebooks uploaded!")

# Delete existing job if it exists
print("\nChecking for existing job...")
existing_jobs = list(w.jobs.list(name="BJs Wholesale Club - Complete Pipeline"))
for job in existing_jobs:
    print(f"  Deleting existing job {job.job_id}...")
    w.jobs.delete(job_id=job.job_id)

# Create the multi-task job
print("\nCreating multi-task job...")

job = w.jobs.create(
    name="BJs Wholesale Club - Complete Pipeline",
    description="""
    Complete data pipeline for BJ's Wholesale Club analytics.

    Tasks:
    1. generate_data - Generate synthetic sales and customer data, create base tables
    2. run_analysis - Run executive analysis queries, create summary tables
    3. generate_insights - Use Claude AI to generate executive insights
    4. generate_charts - Create visualization charts from analysis data
    5. generate_report - Combine insights and charts into PDF/HTML report

    Outputs:
    - Base tables: store_sales_monthly, customer_golden_view
    - Summary tables: exec_summary, store_performance_summary, category_performance_summary, etc.
    - AI insights: ai_executive_insights
    - Charts: 7 PNG visualizations in reports volume
    - Reports: PDF and HTML executive reports

    Can be triggered manually or scheduled.
    """,
    tasks=[
        # Task 1: Generate Data
        Task(
            task_key="generate_data",
            description="Generate synthetic sales and customer data, create base tables in Unity Catalog",
            notebook_task=NotebookTask(
                notebook_path=notebooks["data_pipeline"]["remote"],
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog_name": "{{job.parameters.catalog_name}}",
                    "schema_name": "{{job.parameters.schema_name}}"
                }
            )
        ),
        # Task 2: Run Analysis (depends on Task 1)
        Task(
            task_key="run_analysis",
            description="Run executive analysis queries and create summary tables",
            depends_on=[TaskDependency(task_key="generate_data")],
            notebook_task=NotebookTask(
                notebook_path=notebooks["executive_analysis"]["remote"],
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog_name": "{{job.parameters.catalog_name}}",
                    "schema_name": "{{job.parameters.schema_name}}"
                }
            )
        ),
        # Task 3: Generate AI Insights (depends on Task 2)
        Task(
            task_key="generate_insights",
            description="Use Claude AI to generate executive insights from analysis",
            depends_on=[TaskDependency(task_key="run_analysis")],
            notebook_task=NotebookTask(
                notebook_path=notebooks["ai_insights"]["remote"],
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog_name": "{{job.parameters.catalog_name}}",
                    "schema_name": "{{job.parameters.schema_name}}",
                    "model": "{{job.parameters.model}}",
                    "databricks_host": "{{job.parameters.databricks_host}}",
                    "databricks_token": "{{job.parameters.databricks_token}}"
                }
            )
        ),
        # Task 4: Generate Charts (depends on Task 2 - can run parallel with insights)
        Task(
            task_key="generate_charts",
            description="Generate visualization charts from analysis data",
            depends_on=[TaskDependency(task_key="run_analysis")],
            notebook_task=NotebookTask(
                notebook_path=notebooks["generate_charts"]["remote"],
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog_name": "{{job.parameters.catalog_name}}",
                    "schema_name": "{{job.parameters.schema_name}}"
                }
            )
        ),
        # Task 5: Generate Report (depends on Tasks 3 and 4)
        Task(
            task_key="generate_report",
            description="Combine AI insights and charts into PDF/HTML executive report",
            depends_on=[
                TaskDependency(task_key="generate_insights"),
                TaskDependency(task_key="generate_charts")
            ],
            notebook_task=NotebookTask(
                notebook_path=notebooks["generate_report"]["remote"],
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog_name": "{{job.parameters.catalog_name}}",
                    "schema_name": "{{job.parameters.schema_name}}"
                }
            )
        )
    ],
    parameters=[
        JobParameterDefinition(name="catalog_name", default=CATALOG_NAME),
        JobParameterDefinition(name="schema_name", default=SCHEMA_NAME),
        JobParameterDefinition(name="model", default=MODEL),
        JobParameterDefinition(name="databricks_host", default=DATABRICKS_SERVING_ENDPOINT),
        JobParameterDefinition(name="databricks_token", default=DATABRICKS_TOKEN)
    ],
    tags={
        "project": "bjs-analytics",
        "team": "data-engineering",
        "pipeline_type": "complete",
        "ai_enabled": "true"
    },
    max_concurrent_runs=1
)

print(f"\n{'=' * 70}")
print("JOB CREATED SUCCESSFULLY!")
print(f"{'=' * 70}")
print(f"Job ID: {job.job_id}")
print(f"Job Name: BJs Wholesale Club - Complete Pipeline")
print(f"\nTasks (5-step pipeline):")
print(f"  1. generate_data     - Creates base tables")
print(f"  2. run_analysis      - Creates summary tables (depends on 1)")
print(f"  3. generate_insights - AI-powered insights (depends on 2)")
print(f"  4. generate_charts   - Creates visualizations (depends on 2, parallel with 3)")
print(f"  5. generate_report   - PDF/HTML report (depends on 3 & 4)")
print(f"\nNotebooks:")
for name, paths in notebooks.items():
    print(f"  - {paths['remote']}")
print(f"\nTo run the job:")
print(f"  - UI: {DATABRICKS_HOST}/#job/{job.job_id}")
print(f"  - CLI: databricks jobs run-now {job.job_id}")
print(f"  - SDK: w.jobs.run_now(job_id={job.job_id})")

# Trigger a run
print(f"\n{'=' * 70}")
print("Triggering job run...")
print(f"{'=' * 70}")

run = w.jobs.run_now(job_id=job.job_id)
print(f"Run ID: {run.run_id}")
print(f"Run URL: {DATABRICKS_HOST}/#job/{job.job_id}/run/{run.run_id}")
print("\nJob is now running! Check the Databricks UI for progress.")
print("\nThe pipeline will:")
print("  1. Generate synthetic data (~2 min)")
print("  2. Run analysis queries (~1 min)")
print("  3. Generate AI insights with Claude (~1 min)")
print("  4. Generate charts (~30 sec) [parallel with step 3]")
print("  5. Generate PDF/HTML report (~30 sec)")
print(f"\nFinal output: /Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/reports/")
