from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState
import pendulum
from airflow.models import Variable


@dag(
    dag_id="orchestrate",
    schedule="0 11 * * *",  # codziennie o 11:00
    catchup=False,
    start_date=pendulum.datetime(2026, 9, 20, tz="Europe/Warsaw"),
)
def orchestrate():

    @task
    def data_ingestion():
        ws = WorkspaceClient(
                host=Variable.get("databricks_host"),
                token=Variable.get("databricks_token"),
        )

        run = ws.jobs.run_now_and_wait(
            job_id=194882876441539,
            timeout=timedelta(minutes=30),  # dostosuj do realnego czasu trwania joba
        )

        if run.state.result_state != RunResultState.SUCCESS:
            raise Exception(f"CDC job failed with state: {run.state.result_state}")

        return "data ingested"

    @task.bash
    def clean_target():
         return "rm -rf /opt/airflow/walmart_project/target"
       
    silver = BashOperator(
        task_id='silver',  
        bash_command='cd /opt/airflow/walmart_project && dbt run --select silver'
    )

    silver_test = BashOperator(
        task_id='silver_test',  
        bash_command='cd /opt/airflow/walmart_project && dbt test --select silver'
    )

    snapshot = BashOperator(
        task_id='snapshot',  
        bash_command='cd /opt/airflow/walmart_project && dbt snapshot'
    )    

    gold_dims = BashOperator(
        task_id='gold_dims',  
        bash_command='cd /opt/airflow/walmart_project && dbt run --select gold.dims'
    )

    gold_fact = BashOperator(
        task_id='gold_fact',  
        bash_command='cd /opt/airflow/walmart_project && dbt run --select gold.fact'
    )

    gold_obt = BashOperator(
        task_id='gold_obt',  
        bash_command='cd /opt/airflow/walmart_project && dbt run --select gold --exclude gold.dims gold.fact'
    )

    data_ingestion() >> clean_target() >> silver >> silver_test >> snapshot >> [gold_dims, gold_fact] >> gold_obt


orchestrate_dag = orchestrate()