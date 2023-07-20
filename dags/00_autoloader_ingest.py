from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import timedelta
from airflow.utils.dates import days_ago
import os


# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt"

job_json = 



with DAG(
    dag_id='autoloader_dbt_dag',
    start_date = days_ago(2),
    schedule_interval = None
) as dag:
    
    notebook_run = DatabricksSubmitRunOperator(task_id="autoloader_ingest", json=job_json, databricks_conn_id = 'databricks_conn')
    
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test