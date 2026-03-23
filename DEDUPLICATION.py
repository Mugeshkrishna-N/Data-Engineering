# Databricks notebook source
# Install if not available
%pip install kagglehub

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

os.environ["KAGGLE_USERNAME"] = "mugeshkrishnamk"
os.environ["KAGGLE_KEY"]      = "KGAT_be6c49eaf5ae6f1ea634095ec8143dc2"

# COMMAND ----------

# ============================================================
# STEP 3: Download the dataset
# ============================================================
import kagglehub

# Downloads to a local temp path on the driver node
path = kagglehub.dataset_download("joebeachcapital/metropt-3-dataset")
print("Downloaded to:", path)

# COMMAND ----------

# STEP 4: Inspect files
import os

path = "/home/spark-756842e9-3095-40e8-a4a7-da/.cache/kagglehub/datasets/joebeachcapital/metropt-3-dataset/versions/1"

files = os.listdir(path)
print("Files in dataset:", files)

# COMMAND ----------

# STEP 5: Read CSV with pandas first, then convert to Spark
import pandas as pd

local_path = "/home/spark-756842e9-3095-40e8-a4a7-da/.cache/kagglehub/datasets/joebeachcapital/metropt-3-dataset/versions/1/MetroPT3(AirCompressor).csv"

# Read with pandas (runs on driver, no DBFS needed)
pdf = pd.read_csv(local_path)

print(f"Rows: {len(pdf)} | Columns: {len(pdf.columns)}")
print("Columns:", list(pdf.columns))

# COMMAND ----------

# STEP 6: Convert pandas → Spark DataFrame
df = spark.createDataFrame(pdf)
print(f"Spark Rows: {df.count()} | Columns: {len(df.columns)}")
df.printSchema()

# COMMAND ----------

# STEP 7: Clean column names (drop Unnamed: 0, fix rest)
df = df.drop("Unnamed: 0")  # drop the index column

df = df.toDF(*[
    c.strip().lower()
     .replace(" ", "_")
     .replace("(", "").replace(")", "")
     .replace("/", "_")
     .replace(":", "")
    for c in df.columns
])

print("Cleaned columns:", df.columns)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("bronze_db.metro_sensors_bronze")

# COMMAND ----------

print("Bronze_Metro_Sensors_Data_Created")

# COMMAND ----------

spark.sql("select * from bronze_db.metro_sensors_bronze limit 5").show()

# COMMAND ----------

from pyspark.sql.functions import count,col,sum

bronze_df=spark.table("bronze_db.metro_sensors_bronze")

total_row=bronze_df.count()
distinct_rows=bronze_df.dropDuplicates().count()
print(total_row)
print(distinct_rows)

# COMMAND ----------

from pyspark.sql.functions import count, col, to_timestamp
from pyspark.sql import Window
import pyspark.sql.functions as F

# STEP: Fix timestamp parsing with explicit format
bronze_df = spark.table("bronze_db.metro_sensors_bronze")

# Specify the exact format matching '2020-08-06 07:37:30'
bronze_df = bronze_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# Verify parsing worked — should show 0 nulls
null_ts = bronze_df.filter(col("timestamp").isNull()).count()
print(f"Null timestamps after parsing: {null_ts:,}")

bronze_df.select("timestamp").show(5, truncate=False)

# COMMAND ----------

window= Window.partitionBy("timestamp").orderBy(F.col('timestamp').desc())

silver_df= bronze_df.withColumn("row_number",F.row_number().over(window)).filter(col("row_number")==1).drop("row_number") 

print(f"Bronze rows : {bronze_df.count():,}")
print(f"Silver rows : {silver_df.count():,}")

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_db.metro_sensor_silver")
print("Silver table created ")

# COMMAND ----------

print("***************  END     *********************")

# COMMAND ----------

import kagglehub
import os

# Download dataset
path = kagglehub.dataset_download("joebeachcapital/metropt-3-dataset")

print("Dataset path:", path)

# List files
files = os.listdir(path)
print(files)

# COMMAND ----------

file_path = path + "/MetroPT3(AirCompressor).csv"
print(file_path)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze_db")
spark.sql("CREATE DATABASE IF NOT EXISTS silver_db")

# COMMAND ----------



# COMMAND ----------


import kagglehub

# Download latest version
path = kagglehub.dataset_download("joebeachcapital/metropt-3-dataset")

print("Path to dataset files:", path)

import pandas as pd

url = "https://raw.githubusercontent.com/databricks/LearningSparkV2/master/chapter4/flight-data/csv/2015-summary.csv"

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