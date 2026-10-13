from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable, dag, task
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState

DBT = "cd /opt/airflow/walmart_project && dbt"


@dag(
    dag_id="orchestrate",
    schedule="0 11 * * *",  # daily at 11:00 Europe/Warsaw
    catchup=False,
    start_date=pendulum.datetime(2026, 9, 20, tz="Europe/Warsaw"),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def orchestrate():

    @task
    def data_ingestion():
        """Databricks job: generate source changes -> Lakeflow AUTO CDC pipeline -> walmart.bronze."""
        ws = WorkspaceClient(
            host=Variable.get("databricks_host"),
            token=Variable.get("databricks_token"),
        )
        run = ws.jobs.run_now_and_wait(
            job_id=int(Variable.get("databricks_cdc_job_id", default="194882876441539")),
            timeout=timedelta(minutes=30),
        )
        if run.state.result_state != RunResultState.SUCCESS:
            raise Exception(f"CDC job failed with state: {run.state.result_state}")

    # clean compile + packages on every run (dbt_packages is git-ignored)
    dbt_setup = BashOperator(task_id="dbt_setup", bash_command=f"{DBT} clean && {DBT} deps")

    # fails if Bronze orders / order_items were not refreshed for 2 days
    source_freshness = BashOperator(task_id="source_freshness", bash_command=f"{DBT} source freshness")

    # build = run + test: a failing Silver test stops the DAG before snapshots and Gold
    silver = BashOperator(task_id="silver", bash_command=f"{DBT} build --select silver")
    snapshot = BashOperator(task_id="snapshot", bash_command=f"{DBT} snapshot")
    # sequential: the fact's relationship tests need the dimensions already rebuilt
    gold_dims = BashOperator(task_id="gold_dims", bash_command=f"{DBT} build --select gold.dims")
    gold_fact = BashOperator(task_id="gold_fact", bash_command=f"{DBT} build --select gold.fact")
    gold_obt = BashOperator(task_id="gold_obt", bash_command=f"{DBT} build --select obt")

    (
        data_ingestion()
        >> dbt_setup
        >> source_freshness
        >> silver
        >> snapshot
        >> gold_dims
        >> gold_fact
        >> gold_obt
    )


orchestrate()
