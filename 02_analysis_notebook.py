# Databricks notebook source
df = spark.table("health_project.covid_countries_clean")
df.show(5)


# COMMAND ----------

df.select(
    "cases",
    "deaths",
    "casesPerOneMillion",
    "deathsPerOneMillion",
    "death_rate_pct"
).describe().show()


# COMMAND ----------

top_cases = df.orderBy(df.casesPerOneMillion.desc()).limit(10).toPandas()
top_cases


# COMMAND ----------

top_death_rate = df.orderBy(df.death_rate_pct.desc()).limit(10).toPandas()
top_death_rate


# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(12,6))
plt.bar(top_cases["country"], top_cases["casesPerOneMillion"])
plt.xticks(rotation=45, ha='right')
plt.title("Top 10 Countries by COVID Cases per 1M People")
plt.xlabel("Country")
plt.ylabel("Cases per 1M")
plt.tight_layout()
plt.show()


# COMMAND ----------

plt.figure(figsize=(12,6))
plt.bar(top_death_rate["country"], top_death_rate["death_rate_pct"])
plt.xticks(rotation=45, ha='right')
plt.title("Top 10 Countries by COVID Death Rate (%)")
plt.xlabel("Country")
plt.ylabel("Death Rate (%)")
plt.tight_layout()
plt.show()


# COMMAND ----------

import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

pdf = df.select(
    "cases",
    "deaths",
    "recovered",
    "active",
    "casesPerOneMillion",
    "deathsPerOneMillion",
    "testsPerOneMillion",
    "death_rate_pct"
).toPandas()

plt.figure(figsize=(10,8))
sns.heatmap(pdf.corr(), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap of COVID Metrics")
plt.show()
