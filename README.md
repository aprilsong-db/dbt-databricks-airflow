Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes an example DAG that runs every 30 minutes and simply prints the current date. It also includes an empty 'my_custom_function' that you can fill out to execute Python code.
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
    - in this project, we specified the requirements for the astro-provider-databricks and dbt-databricks
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Getting Started
================

1. Clone this Repo
2. add an .env file locally to specify the dbt/databricks connection details 
    - in this file specify:
        DBT_DATABRICKS_HOST=
        DBT_DATABRICKS_HTTP_PATH=
        DBT_DATABRICKS_TOKEN=
    - you can get the first two from the connection details of your SQL Warehouse 
3. Install Docker
4. Install astro
    - run 'astro dev start' in your terminal
    - this command spins up 4 Docker containers on your machine for each airflow component:
        1. Postgres: Airflow's metadata database
        2. Webserver: The Airflow component responsible for rendering the Airflow UI
        3. Scheduler: The Airflow component responsible for monitoring and triggering tasks
        4. Triggerer: The Airflow component responsible for triggering deferred tasks 
5. Verify that all 4 Docker containers were created by running 'docker ps'
6. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password

Running the DAGS
================
1. In Airflow, start by runninng the 'start_s3_data' DAG. This dag will start the data stream into the S3 bucket. This will be the raw json/csv files that we will then ingest with autoloader
2. Once data has started landing in the S3 bucket, you can start the 'autoloader_dbt_dag' DAG