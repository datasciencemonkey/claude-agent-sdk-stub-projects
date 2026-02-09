"""Create Databricks Job for BJ's Data Pipeline."""
import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, Source,
    JobEmailNotifications, JobNotificationSettings,
    JobParameterDefinition
)
from databricks.sdk.service.compute import ClusterSpec
from databricks.sdk.service.workspace import ImportFormat, Language

load_dotenv()

# Configuration from .env
DATABRICKS_HOST = os.getenv("DATABRICKS_BASE_URL", "").replace("/serving-endpoints", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG_NAME = os.getenv("CATALOG_NAME")
SCHEMA_NAME = os.getenv("SCHEMA_NAME")

# Environment variables for Claude model (if using AI tasks)
CLAUDE_MODEL = os.getenv("MODEL")
CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS = os.getenv("CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS")
ANTHROPIC_CUSTOM_HEADERS = os.getenv("ANTHROPIC_CUSTOM_HEADERS")
DISABLE_PROMPT_CACHING = os.getenv("DISABLE_PROMPT_CACHING")

print("=" * 60)
print("Creating Databricks Job: BJ's Data Pipeline")
print("=" * 60)
print(f"Host: {DATABRICKS_HOST}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")

# Initialize client
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Get current user for workspace path
current_user = w.current_user.me()
username = current_user.user_name
print(f"User: {username}")

# Define workspace path for notebook
workspace_path = f"/Workspace/Users/{username}/jobs"
notebook_path = f"{workspace_path}/bjs_data_pipeline"

# Read the notebook content
with open("jobs/bjs_data_pipeline.py", "r") as f:
    notebook_content = f.read()

# Upload notebook to workspace
print(f"\nUploading notebook to {notebook_path}...")
try:
    # Create directory if needed
    try:
        w.workspace.mkdirs(workspace_path)
    except Exception:
        pass  # Directory may already exist

    # Import notebook
    import base64
    w.workspace.import_(
        path=notebook_path,
        content=base64.b64encode(notebook_content.encode()).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True
    )
    print(f"  Notebook uploaded successfully!")
except Exception as e:
    print(f"  Error uploading notebook: {e}")
    raise

# Create the job
print("\nCreating job...")

job = w.jobs.create(
    name="BJs Wholesale Club - Data Pipeline",
    description="""
    Automated data pipeline for BJ's Wholesale Club analytics.

    This job generates synthetic data and creates the following tables:
    - store_sales_monthly: 18 months of sales by store and category
    - customer_golden_view: Customer master with lifetime value

    Triggered manually or on schedule.
    """,
    tasks=[
        Task(
            task_key="generate_data",
            description="Generate synthetic sales and customer data, create tables",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog_name": CATALOG_NAME,
                    "schema_name": SCHEMA_NAME
                }
            ),
            # Use serverless compute (no cluster config needed)
        )
    ],
    parameters=[
        JobParameterDefinition(name="catalog_name", default=CATALOG_NAME),
        JobParameterDefinition(name="schema_name", default=SCHEMA_NAME)
    ],
    tags={
        "project": "bjs-analytics",
        "team": "data-engineering",
        "data_type": "synthetic"
    },
    max_concurrent_runs=1,
    # Environment variables for any AI/Claude integration
    environments=[
        {
            "environment_key": "default",
            "spec": {
                "client": "1",
                "dependencies": ["faker"]
            }
        }
    ] if False else None  # Disabled for now, using %pip install in notebook
)

print(f"\n{'=' * 60}")
print("JOB CREATED SUCCESSFULLY!")
print(f"{'=' * 60}")
print(f"Job ID: {job.job_id}")
print(f"Job Name: BJs Wholesale Club - Data Pipeline")
print(f"Notebook: {notebook_path}")
print(f"\nTo run the job:")
print(f"  - UI: {DATABRICKS_HOST}/#job/{job.job_id}")
print(f"  - CLI: databricks jobs run-now {job.job_id}")
print(f"  - SDK: w.jobs.run_now(job_id={job.job_id})")

# Optionally trigger a run now
print(f"\n{'=' * 60}")
print("Triggering job run...")
print(f"{'=' * 60}")

run = w.jobs.run_now(job_id=job.job_id)
print(f"Run ID: {run.run_id}")
print(f"Run URL: {DATABRICKS_HOST}/#job/{job.job_id}/run/{run.run_id}")
print("\nJob is now running! Check the Databricks UI for progress.")
