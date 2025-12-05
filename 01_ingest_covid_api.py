# Databricks notebook source
import requests

url = "https://disease.sh/v3/covid-19/countries?allowNull=true"

response = requests.get(url)
print("Status code:", response.status_code)


# COMMAND ----------

data = response.json()

print(type(data), len(data))


# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType
)

# We only take the columns we need for analysis
schema = StructType([
    StructField("country", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("cases", LongType(), True),
    StructField("todayCases", LongType(), True),
    StructField("deaths", LongType(), True),
    StructField("todayDeaths", LongType(), True),
    StructField("recovered", LongType(), True),
    StructField("active", LongType(), True),
    StructField("critical", LongType(), True),
    StructField("casesPerOneMillion", DoubleType(), True),
    StructField("deathsPerOneMillion", DoubleType(), True),
    StructField("tests", LongType(), True),
    StructField("testsPerOneMillion", DoubleType(), True),
    StructField("population", LongType(), True),
])

rows = []

for item in data:
    rows.append((
        item.get("country"),
        item.get("continent"),
        item.get("cases"),
        item.get("todayCases"),
        item.get("deaths"),
        item.get("todayDeaths"),
        item.get("recovered"),
        item.get("active"),
        item.get("critical"),
        item.get("casesPerOneMillion"),
        item.get("deathsPerOneMillion"),
        item.get("tests"),
        item.get("testsPerOneMillion"),
        item.get("population"),
    ))

df_raw = spark.createDataFrame(rows, schema=schema)

df_raw.printSchema()
df_raw.show(5, truncate=False)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS health_project")


# COMMAND ----------

df_raw.write.mode("overwrite").format("delta").saveAsTable("health_project.covid_raw_countries")


# COMMAND ----------

spark.sql("SELECT * FROM health_project.covid_raw_countries LIMIT 5").show()


# COMMAND ----------

df_raw = spark.table("health_project.covid_raw_countries")
df_raw.printSchema()
df_raw.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql import functions as F

df_selected = df_raw.select(
    "country",
    "continent",
    "cases",
    "todayCases",
    "deaths",
    "todayDeaths",
    "recovered",
    "active",
    "critical",
    "casesPerOneMillion",
    "deathsPerOneMillion",
    "tests",
    "testsPerOneMillion",
    "population"
)

df_selected.show(5, truncate=False)


# COMMAND ----------

df_clean = df_selected.filter(
    (F.col("population").isNotNull()) &
    (F.col("population") > 0) &
    (F.col("country").isNotNull())
)


# COMMAND ----------

df_clean = df_clean.withColumn(
    "death_rate_pct",
    (F.col("deaths") / F.col("cases")) * 100
)

df_clean = df_clean.withColumn(
    "active_case_pct",
    (F.col("active") / F.col("cases")) * 100
)


# COMMAND ----------

df_clean.write.mode("overwrite").format("delta").saveAsTable(
    "health_project.covid_countries_clean"
)


# COMMAND ----------

spark.sql("SELECT * FROM health_project.covid_countries_clean LIMIT 5").show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   country,
# MAGIC   continent,
# MAGIC   population,
# MAGIC   cases,
# MAGIC   casesPerOneMillion
# MAGIC FROM health_project.covid_countries_clean
# MAGIC ORDER BY casesPerOneMillion DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   country,
# MAGIC   continent,
# MAGIC   cases,
# MAGIC   deaths,
# MAGIC   ROUND(death_rate_pct, 2) AS death_rate_pct
# MAGIC FROM health_project.covid_countries_clean
# MAGIC WHERE cases > 10000   -- avoid small-sample noise
# MAGIC ORDER BY death_rate_pct DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM health_project.covid_countries_clean LIMIT 20;
# MAGIC