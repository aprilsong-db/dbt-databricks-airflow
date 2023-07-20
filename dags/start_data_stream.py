from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)
from airflow.utils.dates import days_ago

DATABRICKS_CONN_ID = "databricks_conn"
TARGET_S3_PATH = "s3://oetrta/asong/dbdemos/dbt-retail/" #TODO: change to airflow run config

default_args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "depends_on_past": False,
}

data_stream_job = {
        "name": "start_s3_data_stream",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "start_s3_data_stream",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "dags/databricks/01-ingest-autoloader/00-start-data-stream",
                    "source": "GIT",
                    "base_parameters": {"base_s3_path":TARGET_S3_PATH}
                },
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "13.2.x-scala2.12",
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "us-west-2a",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "enable_elastic_disk": False,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 1
                },
                "timeout_seconds": 3600,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False
                }
            }
        ],
        "git_source": {
            "git_url": "https://github.com/aprilsong-db/dbt-databricks-airflow",
            "git_provider": "gitHub",
            "git_branch": "main"
        },
        "format": "MULTI_TASK"
    }


with DAG(
    dag_id="start_data_stream",
    start_date=days_ago(2),
    schedule_interval=None,
    default_args=default_args,
) as dag:
    
    databricks_ingest = DatabricksSubmitRunOperator(
        task_id="databricks_ingest",
        json=data_stream_job,
        databricks_conn_id=DATABRICKS_CONN_ID,
    )

    databricks_ingest