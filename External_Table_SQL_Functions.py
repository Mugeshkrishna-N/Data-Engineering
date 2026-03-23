# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS Primary_DB")

# COMMAND ----------

spark.sql("USE Primary_DB")

# COMMAND ----------

import pandas as pd

url = "https://docs.google.com/spreadsheets/d/1VmzRC91edaUsxGm9mElR2CWNMM1RMI-TZ6ZETLPvnf0/export?format=csv&gid=335252259"

pdf = pd.read_csv(url)

df = spark.createDataFrame(pdf)

# 🔥 Clean column names
df = df.toDF(*[
    col.strip()
       .lower()
       .replace(" ", "_")
       .replace("(", "")
       .replace(")", "")
       .replace("/", "_")
    for col in df.columns
])

# Save table
df.write.format("delta").mode("overwrite").saveAsTable("shopping_external_table")

# Show data
df.show(10)

# COMMAND ----------


# Customers Who are all purchased more than $10

df = spark.table("Primary_DB.shopping_external_table")

high_value_df = df.groupBy("customer_id") \
    .sum("purchase_amount_usd") \
    .withColumnRenamed("sum(purchase_amount_usd)", "total_spent") \
    .filter("total_spent > 10")

high_value_df.show()

# COMMAND ----------

# Each category sales cost - Revenue Per Category

from pyspark.sql.functions import sum

category_df = df.groupBy("category") \
    .agg(sum("purchase_amount_usd").alias("total_revenue")) \
    .orderBy("total_revenue", ascending=False)

category_df.show()

# COMMAND ----------

# How Frequent customer purchases

freq_df = df.groupBy("frequency_of_purchases") \
    .count() \
    .orderBy("count", ascending=False)

freq_df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_with_time = df.withColumn("ingestion_time", current_timestamp())

df_with_time.show(5)