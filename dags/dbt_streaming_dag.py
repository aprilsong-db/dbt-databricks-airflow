from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
    DatabricksRunNowOperator,
)
from airflow.utils.dates import timedelta
from airflow.utils.dates import days_ago
import os


# We're hardcoding this value here for the purpose of the demo, but in a production environment this
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR")
DATABRICKS_CONN_ID = os.environ.get("AIRFLOW_DATABRICKS_CONN_ID")
S3_BASE_PATH = os.environ.get("S3_BASE_PATH")
DBT_TARGET_UC_CATALOG = os.environ.get("DBT_TARGET_UC_CATALOG")
DBT_TARGET_UC_SCHEMA = os.environ.get("DBT_TARGET_UC_SCHEMA")


ml_churn_pred_job = {
    "name": "churn-prediction",
    "email_notifications": {"no_alert_for_skipped_runs": False},
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "churn-prediction",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "dags/databricks/03-churn-prediction",
                "source": "GIT",
            },
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "12.2.x-cpu-ml-scala2.12",
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "us-west-2a",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0,
                },
                "node_type_id": "i3.xlarge",
                "enable_elastic_disk": False,
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
                "num_workers": 2,
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False,
            },
        }
    ],
    "git_source": {
        "git_url": "https://github.com/aprilsong-db/dbt-databricks-airflow",
        "git_provider": "gitHub",
        "git_branch": "main",
    },
    "format": "MULTI_TASK",
}


with DAG(
    dag_id="dbt_streaming_dag",
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:

    # dbt_ingest = BashOperator(
    #     task_id="dbt_ingest",
    #     bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    # )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    databricks_churn_prediction = DatabricksSubmitRunOperator(
        task_id="databricks_ml_churn_pred",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=ml_churn_pred_job,
    )

    dbt_run >> dbt_test >> databricks_churn_prediction
