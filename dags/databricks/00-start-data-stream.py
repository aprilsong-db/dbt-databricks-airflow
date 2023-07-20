# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook.
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %pip install Faker


# COMMAND ----------

import time
from generate_data import *
import generate_data as util
import os
from config import S3_BASE_PATH, DATA_FREQUENCY_SECONDS


while True:
    batch_customer_df = util.generate_customer_data()
    cust_ids = batch_customer_df.select("id").collect()
    cust_ids = [r["id"] for r in cust_ids]

    batch_orders_df = util.create_orders_data(
        cust_ids, os.path.join(S3_BASE_PATH, "orders")
    )
    batch_events_df = util.create_events_data(
        cust_ids, os.path.join(S3_BASE_PATH, "events")
    )
    batch_users_df = util.create_users_data(
        batch_customer_df,
        batch_events_df,
        batch_orders_df,
        os.path.join(S3_BASE_PATH, "users"),
    )
    time.sleep(DATA_FREQUENCY_SECONDS)
