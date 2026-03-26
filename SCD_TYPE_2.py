# Databricks notebook source
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_db.flight_bronze_scd2 AS
SELECT * FROM bronze_db.flight_bronze
""")

# COMMAND ----------

spark.sql("select * from bronze_db.flight_bronze_scd2 limit 100 ").show()

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM bronze_db.flight_bronze_scd2").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

bronze_df = spark.table("bronze_db.flight_bronze_scd2")

window_spec = Window.partitionBy("id").orderBy(col("timestamp").desc())

latest_df = bronze_df.withColumn("rn", row_number().over(window_spec)) \
    .filter("rn = 1") \
    .drop("rn")

# COMMAND ----------

from pyspark.sql.functions import lit

scd_init_df = latest_df \
    .withColumn("start_date", col("timestamp")) \
    .withColumn("end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

# COMMAND ----------

scd_init_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_db.flight_scd2")

# COMMAND ----------

spark.sql("SELECT * FROM silver_db.flight_scd2 LIMIT 10").show()

# COMMAND ----------

from datetime import datetime, timedelta
import random

new_data = []

for i in range(1, 50):  # simulate updates
    new_data.append((
        random.randint(1, 500),  # existing ids
        datetime.now() + timedelta(minutes=random.randint(1, 1000)),
        random.choice(["Chennai","Delhi","Mumbai"]),
        random.choice(["Dubai","Paris","London"]),
        random.choice(["AI101","EK202"]),
        random.choice(["U","D"])
    ))

columns = ["id","timestamp","origin","destination","flight_name","op"]

new_df = spark.createDataFrame(new_data, columns)

# Append to bronze
new_df.write.format("delta") \
    .mode("append") \
    .saveAsTable("bronze_db.flight_bronze_scd2")

# COMMAND ----------

from pyspark.sql.functions import max

last_ts = spark.table("silver_db.flight_scd2") \
    .agg(max("timestamp")) \
    .collect()[0][0]

bronze_df = spark.table("bronze_db.flight_bronze_scd2")

incremental_df = bronze_df.filter(col("timestamp") > last_ts)

# COMMAND ----------

window_spec = Window.partitionBy("id").orderBy(col("timestamp").desc())

latest_updates_df = incremental_df.withColumn("rn", row_number().over(window_spec)) \
    .filter("rn = 1") \
    .drop("rn")

# COMMAND ----------

 scd_updates_df = latest_updates_df \
    .withColumn("start_date", col("timestamp")) \
    .withColumn("end_date", lit(None).cast("timestamp")) \
    .withColumn("is_current", lit(True))

# COMMAND ----------

from delta.tables import DeltaTable

scd_table = DeltaTable.forName(spark, "silver_db.flight_scd2")

# 1️⃣ Expire old records
scd_table.alias("target").merge(
    scd_updates_df.alias("source"),
    "target.id = source.id AND target.is_current = true"
).whenMatchedUpdate(
    condition="""
        source.op IN ('U','D') AND (
            target.origin != source.origin OR
            target.destination != source.destination OR
            target.flight_name != source.flight_name
        )
    """,
    set={
        "end_date": "source.timestamp",
        "is_current": "false"
    }
).execute()

# 2️⃣ Insert new rows
scd_updates_df.filter("op IN ('I','U')") \
    .write.format("delta") \
    .mode("append") \
    .saveAsTable("silver_db.flight_scd2")

# COMMAND ----------

spark.sql("""
SELECT *
FROM silver_db.flight_scd2 where id='20'
ORDER BY id, start_date
""").show(50)