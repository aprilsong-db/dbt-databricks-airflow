Overview
========

This demo shows how to 
1. incrementally ingest raw data files from cloud storage into `bronze` Delta tables using Databricks Autoloader
1. orchestrate ingestion and dbt transformations with Airflow on Databricks
1. load an ML model from MLflow as a SQL function after dbt transformations are complete and applied to `dbt_c360_gold_churn_features`

Project Contents
================

This project uses Astronomer to set up a project contains the following files and folders:

- `dags`: This folder contains the Python files for your Airflow DAGs. 
    - `/databricks`: contains scripts to set up an example data stream of raw json and csv files landing into S3 (00-start-data-stream.py), ingest the raw data using autoloader (01-data-ingestion), and generate predictions using a machine learning model (03-churn-prediction)
    - `/dbt`: contains a dbt project with example dbt models to build a pipeline to move the data through the medallion architecture (bronze -> silver -> gold)
- `Dockerfile`: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- `include`: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- `packages.txt`: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- `requirements.txt`: Install Python packages needed for your project by adding them to this file. It is empty by default.
    - in this project, we specified the requirements for the `astro-provider-databricks` and `dbt-databricks`
- `plugins`: Add custom or community plugins for your project to this file. It is empty by default.
- `airflow_settings.yaml`: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.


Pre-requisites
================
- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://docs.astronomer.io/astro/cli/overview)
- Read/Write access to an S3 bucket
- Access to a [Unity Catalog enabled](https://docs.databricks.com/data-governance/unity-catalog/enable-workspaces.html) Databricks Workspace
    - [Create](https://docs.databricks.com/data-governance/unity-catalog/create-schemas.html) a target `catalog` and `schema` in Unity Catalog where dbt models will written
    - Create a [storage credential](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#create-a-storage-credential) and [external location](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#manage-external-locations) to access your S3 bucket 
- Access to a [Databricks SQL Warehouse](https://docs.databricks.com/sql/admin/create-sql-warehouse.html)
    - Size `M` Serverless SQL Warehouse is recommended
- [Databricks personal access token](https://docs.databricks.com/dev-tools/auth.html#databricks-personal-access-token-authentication) for authentication
    - This token will be used to authenticate to Databricks from dbt and Airflow


Local Setup
================

1. Clone this repo and cd into the project directory
```sh
git clone https://github.com/aprilsong-db/dbt-databricks-airflow.git
cd dbt-databricks-airflow
```
2. Create an `.env` file locally to specify the dbt/databricks connection details
```sh 
cat <<EOF > .env
DBT_DATABRICKS_HOST=<YOUR-DATABRICKS-HOST>
DBT_DATABRICKS_HTTP_PATH=<YOUR-DATABRICKS-HTTP-PATH>
DBT_DATABRICKS_TOKEN=<YOUR-DATABRICKS-TOKEN>
DBT_TARGET_UC_CATALOG=<YOUR-TARGET-UC-CATALOG>
DBT_TARGET_UC_SCHEMA=<YOUR-TARGET-UC-CATALOG>
DBT_PROJECT_DIR=/usr/local/airflow/dags/dbt
AIRFLOW_DATABRICKS_CONN_ID=<YOUR-AIRFLOW-DATABRICKS-CONN>
S3_BASE_PATH=<YOUR-S3-PATH>
DATA_FREQUENCY_SECONDS=600
DATA_STREAM_TIMEOUT_SECONDS=3600
EOF
```  
-  `DBT_DATABRICKS_HOST`: the hostname of your Databricks workspace, without the protocol (e.g. https://).
- `DBT_DATABRICKS_HTTP_PATH`: the HTTP path of your Databricks workspace
- `DBT_DATABRICKS_TOKEN`: your Databricks personal access token.
- `DBT_TARGET_UC_CATALOG`: the name of your catalog in Unity Catalog where your DBT models will be deployed
- `DBT_TARGET_UC_SCHEMA`: the name of the schema within your UC catalog where your DBT models will be deployed
- `DBT_PROJECT_DIR`: the directory that contains dbt models
- `AIRFLOW_DATABRICKS_CONN_ID`: Conn ID of your Databricks connection in Airflow. You will use this value to create a Databricks Connection in the Airflow UI in step 5 of Local Setup. 
- `S3_BASE_PATH`: the base path of your S3 bucket where the data stream will write raw files (csv, json) for ingestion. This is the external location in Unity Catalog. 
- `DATA_FREQUENCY_SECONDS`: the amount of time (in seconds) between data refreshes. This value is used to set up and simulate a data stream for this demo. The default value is 600 seconds (10 minutes).
- `DATA_STREAM_TIMEOUT_SECONDS`: amount of time (in seconds) after which the simulated data stream will shut down. Default is 1 hour.

    Resources:
    - [Get connection details for a SQL warehouse
    ](https://docs.databricks.com/integrations/jdbc-odbc-bi.html#get-connection-details-for-a-sql-warehouse)  

3. Start Airflow locally with `astro`.
```sh
astro dev start
```
4. Access the Airflow UI `http://localhost:8080/` and login
    - username: `admin`
    - passowrd: `admin` 
5. Create a [Databricks connection](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html) in `Admin > Connections` using the value you set for `AIRFLOW_DATABRICKS_CONN_ID` in your `.env` file.
    - `host`: Databricks workspace URL
    - `password`: Your Databricks Token

Running this demo
================
1. Trigger the `start_s3_data` DAG to start and simulate data stream representing users, orders, and event data to your S3 bucket. This will be the raw json/csv files that we will then ingest with autoloader. Job will automatically timeout and end after 1 hour - you can update `timeout_seconds` in [/dags/start_data_stream.py](/dags/start_data_stream.py).
2. Trigger the `autoloader_dbt_dag` DAG once data has started landing in your S3 bucket. All new files available at run time will be ingested. The corresponding [Databricks job](dags/databricks/01-data-ingestion.py) run will show counts of the raw bronze tables after each ingestion. Trigger the job again to see and confirm new files that arrived after the first run have been incrementally ingested. Note - there are no changes in number of users in this demo. 

To shut down Airflow, run
```sh
astro dev stop
```