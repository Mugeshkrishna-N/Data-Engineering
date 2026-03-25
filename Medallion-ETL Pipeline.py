# Databricks notebook source
import random
from datetime import datetime, timedelta

num_records = 1000

origins = ["Chennai", "Bangalore", "Delhi", "Mumbai", "Hyderabad"]
destinations = ["Dubai", "Singapore", "London", "New York", "Paris"]
flights = ["AI101", "EK202", "SQ303", "BA404", "LH505"]
operations = ["I", "U", "D"]

data = []

base_time = datetime.now()

for i in range(1, num_records + 1):
    data.append((
        i,
        base_time - timedelta(minutes=random.randint(0, 5000)),
        random.choice(origins),
        random.choice(destinations),
        random.choice(flights),
        random.choice(operations)
    ))

for _ in range(200):
    row = random.choice(data)
    data.append((
        row[0],  # same id
        row[1] + timedelta(minutes=random.randint(1, 100)),  # new timestamp also
        row[2],
        row[3],
        row[4],
        random.choice(["U", "D"])
    ))

# COMMAND ----------

columns = ["id", "timestamp", "origin", "destination", "flight_name", "op"]

df = spark.createDataFrame(data, columns)

df.show(5)

# COMMAND ----------

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("bronze_db.flight_bronze")

# COMMAND ----------

spark.sql("SELECT * FROM bronze_db.flight_bronze LIMIT 10").show()

# COMMAND ----------

spark.sql("Select id,count(*) as cd from bronze_db.flight_bronze group by id").show()

# COMMAND ----------

spark.sql("Select * from bronze_db.flight_bronze where op='D' ").show()

# COMMAND ----------

bronze_df = spark.table("bronze_db.flight_bronze")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("id").orderBy(col("timestamp").desc())

latest_df = bronze_df.withColumn("rn", row_number().over(window_spec)) \
    .filter("rn = 1") \
    .drop("rn")

# COMMAND ----------

inserts_updates_df = latest_df.filter("op IN ('I','U')")
deletes_df = latest_df.filter("op = 'D'")

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql("CREATE DATABASE IF NOT EXISTS silver_db")

try:
    silver_table = DeltaTable.forName(spark, "silver_db.flight_silver")


    silver_table.alias("target").merge(
        inserts_updates_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()


    silver_table.alias("target").merge(
        deletes_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedDelete() \
     .execute()

except:
  
    inserts_updates_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_db.flight_silver")

# COMMAND ----------

spark.sql("select id,count(*) as cd from silver_db.flight_silver group by id order by cd desc").show()

# COMMAND ----------

spark.sql("select * from silver_db.flight_silver where op='D'").show()

# COMMAND ----------

import random
from datetime import datetime, timedelta

# Existing max id (from previous batch)
existing_max_id = num_records  # 1000

new_data = []

current_time = datetime.now()


for _ in range(300): 
    existing_id = random.randint(1, existing_max_id)

    new_data.append((
        existing_id,
        current_time + timedelta(minutes=random.randint(1, 500)),  
        random.choice(origins),
        random.choice(destinations),
        random.choice(flights),
        random.choice(["U", "D"])  # update / delete operations
    ))

for i in range(existing_max_id + 1, existing_max_id + 201): 
    new_data.append((
        i,
        current_time + timedelta(minutes=random.randint(1, 500)),
        random.choice(origins),
        random.choice(destinations),
        random.choice(flights),
        "I"  #insert operations
    ))

incremental_data = new_data

# COMMAND ----------

columns = ["id", "timestamp", "origin", "destination", "flight_name", "op"]

incremental_df = spark.createDataFrame(incremental_data, columns)

incremental_df.show(10)

# COMMAND ----------

incremental_df.write.format("delta") \
  .mode("append") \
  .saveAsTable("bronze_db.flight_bronze")

# COMMAND ----------

spark.sql("select max(timestamp) from bronze_db.flight_bronze").show()

# COMMAND ----------

from pyspark.sql.functions import max

try:
    last_ts = spark.table("silver_db.flight_silver") \
        .agg(max("timestamp")) \
        .collect()[0][0]
except:
    last_ts = None

print("Last processed timestamp:", last_ts)

# COMMAND ----------

bronze_df = spark.table("bronze_db.flight_bronze")

if last_ts:
    incremental_df = bronze_df.filter(bronze_df.timestamp > last_ts)
else:
    incremental_df = bronze_df  # first load

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("id").orderBy(col("timestamp").desc())

latest_df = incremental_df.withColumn("rn", row_number().over(window_spec)) \
    .filter("rn = 1") \
    .drop("rn")

# COMMAND ----------

from delta.tables import DeltaTable

try:
    silver_table = DeltaTable.forName(spark, "silver_db.flight_silver")

    silver_table.alias("target").merge(
        latest_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll(condition="source.op IN ('I','U')") \
     .whenMatchedDelete(condition="source.op = 'D'") \
     .whenNotMatchedInsertAll(condition="source.op IN ('I','U')") \
     .execute()

except:
    latest_df.filter("op IN ('I','U')") \
        .write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_db.flight_silver")

# COMMAND ----------

spark.sql("select id,count(*) as cd from silver_db.flight_silver group by id order by cd desc").show()

# COMMAND ----------

spark.sql("select * from silver_db.flight_silver where op='D'").show()