from prefect import Flow
from prefect.storage import Git

storage = Git(
    repo="data-engineering-zoomcamp",
    flow_path="week_2_workflow_orchestration/prefect/etl_web_to_gcs.py",
    repo_host="github.com",
    use_ssh=True,
)
