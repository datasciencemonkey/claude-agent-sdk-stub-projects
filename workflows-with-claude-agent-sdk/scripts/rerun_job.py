"""Re-upload notebook and trigger job run."""
import os
import base64
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_BASE_URL", "").replace("/serving-endpoints", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# Get current user
current_user = w.current_user.me()
username = current_user.user_name
notebook_path = f"/Workspace/Users/{username}/jobs/bjs_data_pipeline"

# Read and upload updated notebook
print("Uploading updated notebook...")
with open("jobs/bjs_data_pipeline.py", "r") as f:
    notebook_content = f.read()

w.workspace.import_(
    path=notebook_path,
    content=base64.b64encode(notebook_content.encode()).decode(),
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
    overwrite=True
)
print(f"  Notebook updated: {notebook_path}")

# Trigger job run
JOB_ID = 52943363988896
print(f"\nTriggering job {JOB_ID}...")
run = w.jobs.run_now(job_id=JOB_ID)
print(f"Run ID: {run.run_id}")
print(f"Run URL: {DATABRICKS_HOST}/#job/{JOB_ID}/run/{run.run_id}")
