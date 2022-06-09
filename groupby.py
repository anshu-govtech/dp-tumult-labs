import os
import requests
from pyspark.sql import SparkSession
from tmlt.analytics.keyset import KeySet
from tmlt.analytics.privacy_budget import PureDPBudget
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.session import Session

r = requests.get(
    'https://tumult-public.s3.amazonaws.com/library-members.csv',
)
with open("members.csv", "w") as f:
    f.write(r.text)
spark = SparkSession.builder.getOrCreate()
members_df = spark.read.csv("members.csv", header=True, inferSchema=True)

session = Session.from_dataframe(
    privacy_budget=PureDPBudget(epsilon=float('inf')),
    source_id="members",
    dataframe=members_df,
)

edu_levels = KeySet.from_dict({
    "education_level": [
        "up-to-high-school",
        "high-school-diploma",
        "bachelors-associate",
        "masters-degree",
        "doctorate-professional",
    ]
})

edu_average_age_query = (
    QueryBuilder("members")
    .groupby(edu_levels)
    .average("age", low=0, high=120)
)
edu_average_ages = session.evaluate(
    edu_average_age_query,
    privacy_budget=PureDPBudget(1),
)
edu_average_ages.sort("age_average").show(truncate=False)

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")
g = sns.barplot(
    x="education_level",
    y="age_average",
    data=edu_average_ages.toPandas().sort_values("age_average"),
    color="#1f77b4",
)
g.set_xticklabels(g.get_xticklabels(), rotation=45, horizontalalignment="right")
plt.title("Average age of library members, by education level")
plt.xlabel("Education level")
plt.ylabel("Average age")
plt.tight_layout()
plt.show()

