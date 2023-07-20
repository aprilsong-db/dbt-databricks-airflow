from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from faker import Faker
from collections import OrderedDict 
import uuid
import random
from datetime import datetime, timedelta



fake = Faker()
fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_date_old = F.udf(lambda:fake.date_between_dates(date_start=datetime(2012,1,1), date_end=datetime(2015,12,31)).strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
canal = OrderedDict([("WEBAPP", 0.5),("MOBILE", 0.1),("PHONE", 0.3),(None, 0.01)])
fake_canal = F.udf(lambda:fake.random_elements(elements=canal, length=1)[0])
fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)
countries = ['FR', 'USA', 'SPAIN']
fake_country = F.udf(lambda: countries[random.randint(0,2)])


spark = SparkSession.getActiveSession()

def generate_customer_data():
  def fake_date_between(months=0):
    start = datetime.now() - timedelta(days=30*months)
    return F.udf(lambda: fake.date_between_dates(date_start=start, date_end=start + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S"))

  def get_df(size, month):
    df = spark.range(0, size).repartition(10)
    df = df.withColumn("id", fake_id())
    df = df.withColumn("firstname", fake_firstname())
    df = df.withColumn("lastname", fake_lastname())
    df = df.withColumn("email", fake_email())
    df = df.withColumn("address", fake_address())
    df = df.withColumn("canal", fake_canal())
    df = df.withColumn("country", fake_country())  
    df = df.withColumn("creation_date", fake_date_between(month)())
    df = df.withColumn("last_activity_date", fake_date())
    df = df.withColumn("gender", F.round(F.rand()+0.2))
    return df.withColumn("age_group", F.round(F.rand()*10))

  df_customers = get_df(133, 12*30).withColumn("creation_date", fake_date_old())

  from concurrent.futures import ThreadPoolExecutor

  def gen_data(i):
      return get_df(2000+i*200, 24-i)

  with ThreadPoolExecutor(max_workers=3) as executor:
      for n in executor.map(gen_data, range(1, 24)):
          df_customers = df_customers.union(n)

  df_customers = df_customers.cache()
  return df_customers


def create_orders_data(customer_ids, path):
    orders = spark.createDataFrame([(i,) for i in customer_ids], ['user_id'])
    orders = orders.withColumn("id", fake_id())
    orders = orders.withColumn("transaction_date", fake_date())
    orders = orders.withColumn("item_count", F.round(F.rand()*2)+1)
    orders = orders.withColumn("amount", F.col("item_count")*F.round(F.rand()*30+10))
    orders = orders.withColumn("insert_timestamp", F.current_timestamp()) \
            .withColumn("year", F.year(F.col("insert_timestamp"))) \
            .withColumn("month", F.month(F.col("insert_timestamp"))) \
            .withColumn("day", F.dayofmonth(F.col("insert_timestamp"))) \
            .withColumn("hour", F.hour(F.col("insert_timestamp"))) \
            .withColumn("minute", F.minute(F.col("insert_timestamp"))) 
    orders = orders.cache()
    orders.write.partitionBy("year", "month", "day", "hour", "minute").format("json").mode("overwrite").save(path)
    print(f"{datetime.now()}: Orders written to {path}")
    return orders


def create_events_data(customer_ids, path):
    import re

    platform = OrderedDict([("ios", 0.5),("android", 0.1),("other", 0.3),(None, 0.01)])
    fake_platform = F.udf(lambda:fake.random_elements(elements=platform, length=1)[0])

    action_type = OrderedDict([("view", 0.5),("log", 0.1),("click", 0.3),(None, 0.01)])
    fake_action = F.udf(lambda:fake.random_elements(elements=action_type, length=1)[0])
    fake_uri = F.udf(lambda:re.sub(r'https?:\/\/.*?\/', "https://databricks.com/", fake.uri()))


    actions = spark.createDataFrame([(i,) for i in customer_ids], ['user_id']).repartition(20)
    actions = actions.withColumn("event_id", fake_id())
    actions = actions.withColumn("platform", fake_platform())
    actions = actions.withColumn("date", fake_date())
    actions = actions.withColumn("action", fake_action())
    actions = actions.withColumn("session_id", fake_id())
    actions = actions.withColumn("url", fake_uri())
    actions = actions.withColumn("insert_timestamp", F.current_timestamp()) \
            .withColumn("year", F.year(F.col("insert_timestamp"))) \
            .withColumn("month", F.month(F.col("insert_timestamp"))) \
            .withColumn("day", F.dayofmonth(F.col("insert_timestamp"))) \
            .withColumn("hour", F.hour(F.col("insert_timestamp"))) \
            .withColumn("minute", F.minute(F.col("insert_timestamp"))) 
    actions = actions.cache()
    actions.write.partitionBy("year", "month", "day", "hour", "minute").format("csv").option("header", True).mode("overwrite").save(path)
    print(f"{datetime.now()}: Orders written to {path}")
    return actions

def create_users_data(batch_customers_df, batch_events_df, batch_orders_df, path):
    churn_proba_action = batch_events_df.groupBy('user_id').agg({'platform': 'first', '*': 'count'}).withColumnRenamed("count(1)", "action_count")
    #Let's count how many order we have per customer.
    churn_proba = batch_orders_df.groupBy('user_id').agg({'item_count': 'sum', '*': 'count'})
    churn_proba = churn_proba.join(churn_proba_action, ['user_id'])
    churn_proba = churn_proba.join(batch_customers_df, churn_proba.user_id == batch_customers_df.id)

    #Customer having > 5 orders are likely to churn

    churn_proba = (churn_proba.withColumn("churn_proba", 5 +  F.when(((F.col("count(1)") >=5) & (F.col("first(platform)") == "ios")) |
                                                                    ((F.col("count(1)") ==3) & (F.col("gender") == 0)) |
                                                                    ((F.col("count(1)") ==2) & (F.col("gender") == 1) & (F.col("age_group") <= 3)) |
                                                                    ((F.col("sum(item_count)") <=1) & (F.col("first(platform)") == "android")) |
                                                                    ((F.col("sum(item_count)") >=10) & (F.col("first(platform)") == "ios")) |
                                                                    (F.col("action_count") >=4) |
                                                                    (F.col("country") == "USA") |
                                                                    ((F.datediff(F.current_timestamp(), F.col("creation_date")) >= 90)) |
                                                                    ((F.col("age_group") >= 7) & (F.col("gender") == 0)) |
                                                                    ((F.col("age_group") <= 2) & (F.col("gender") == 1)), 80).otherwise(20)))

    churn_proba = churn_proba.withColumn("churn", F.rand()*100 < F.col("churn_proba"))
    churn_proba = churn_proba.drop("user_id", "churn_proba", "sum(item_count)", "count(1)", "first(platform)", "action_count")
    churn_proba = churn_proba.withColumn("insert_timestamp", F.current_timestamp()) \
        .withColumn("year", F.year(F.col("insert_timestamp"))) \
        .withColumn("month", F.month(F.col("insert_timestamp"))) \
        .withColumn("day", F.dayofmonth(F.col("insert_timestamp"))) \
        .withColumn("hour", F.hour(F.col("insert_timestamp"))) \
        .withColumn("minute", F.minute(F.col("insert_timestamp"))) 
    churn_proba.write.partitionBy("year", "month", "day", "hour", "minute").format("json").mode("overwrite").save(path)
    # cleanup_folder(folder+"/users")
    print(f"{datetime.now()}: Orders written to {path}")
    return churn_proba
  
