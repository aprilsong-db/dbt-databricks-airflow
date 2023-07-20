# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

#dbutils.widgets.text("raw_data_location", "/demos/retail/churn/", "Raw data location (stating dir)")
dbutils.widgets.text("base_s3_path", "YOUR_S3_PATH", "Base S3 Directory")
base_s3_path = dbutils.widgets.get("base_s3_path")

# COMMAND ----------

import json
import time
import boto3
from datetime import datetime
from generate_data import *

# Use this if not using UC enabled cluster to simulate data stream to s3.
# SECRET_SCOPE = "database_secrets_asong"
# AWS_ACCESS_KEY_ID = dbutils.secrets.get(scope=SECRET_SCOPE, key="AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key="AWS_SECRET_ACCESS_KEY")
# AWS_SESSION_TOKEN = dbutils.secrets.get(scope=SECRET_SCOPE, key="AWS_SESSION_TOKEN")
# YOUR_REGION = 'us-west-2'


# s3 = boto3.client('s3',
#                   aws_access_key_id=AWS_ACCESS_KEY_ID,
#                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#                   aws_session_token=AWS_SESSION_TOKEN)

# spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
# spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
# spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)    

# COMMAND ----------

import generate_data as util
import time

data_freq_seconds = 120
while True:
    batch_customer_df = util.generate_customer_data()
    cust_ids = batch_customer_df.select("id").collect()
    cust_ids = [r["id"] for r in cust_ids]

    batch_orders_df = util.create_orders_data(cust_ids, f"{base_s3_path}/orders")
    batch_events_df = util.create_events_data(cust_ids, f"{base_s3_path}/events")
    batch_users_df = util.create_users_data(batch_customer_df, batch_events_df, batch_orders_df, f"{base_s3_path}/users")
    time.sleep(data_freq_seconds)

# COMMAND ----------


