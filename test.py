import os
import requests
from pyspark.sql import SparkSession
from tmlt.analytics.privacy_budget import PureDPBudget
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.session import Session

print("Anshu")

r = requests.get(
    'https://tumult-public.s3.amazonaws.com/library-members.csv',
)

spark = (
    SparkSession.builder
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    .getOrCreate()
)

with open("members.csv", "w") as f:
    f.write(r.text)
members_df = spark.read.csv("members.csv", header=True, inferSchema=True)


session = Session.from_dataframe(
    privacy_budget=PureDPBudget(3),
    source_id="members",
    dataframe=members_df
)

count_query = QueryBuilder("members").count()

total_count = session.evaluate(
    count_query,
    privacy_budget=PureDPBudget(epsilon=1)
)

print("noisy value------")
total_count.show()

print("true value------")
total_count = members_df.count()
print(total_count)