from airflow.utils.state import State
from airflow.models import DagRun
from airflow.exceptions import AirflowSkipException

def skip_if_not_latest_run(**context):
    """Skips the current run if it's not the latest DAG run or if another run is still running."""
    current_run = context["dag_run"]
    dag_runs = DagRun.find(dag_id=current_run.dag_id)

    latest_run = max(dag_runs, key=lambda run: run.execution_date)
    is_latest = current_run.run_id == latest_run.run_id
    is_any_running = any(
        run.state == State.RUNNING and run.run_id != current_run.run_id
        for run in dag_runs
    )

    if not is_latest or is_any_running:
        raise AirflowSkipException("Skipping: Not the latest run or another run is active.")