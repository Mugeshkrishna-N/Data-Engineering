from pyspark.sql import SparkSession
from datetime import datetime
import pytz

def main():
    # Step 1: Get current IST time
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist)
    hour = current_time.hour
    minute = current_time.minute

    if hour == 16 and minute == 45:
        # Step 2: Create Spark session
        spark = SparkSession.builder \
            .appName("Scheduled Replication Task") \
            .config("spark.jars", "/Users/mugesh_krishna/Downloads/mysql-connector-j-9.3.0/mysql-connector-j-9.3.0.jar") \
            .getOrCreate()

        print("Starting replication at 15:00 IST...")

        # JDBC connection info
        jdbc_url = "jdbc:mysql://localhost:3306/samplebill"
        properties = {
            "user": "root",
            "password": "mugesh585",
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Step 3: Read source and target tables
        df = spark.read.jdbc(url=jdbc_url, table="billinfo", properties=properties)
        repli_df = spark.read.jdbc(url=jdbc_url, table="billrepli", properties=properties)

        # Step 4: Find new records not in billrepli
        repli_ids_df = repli_df.select("billid").distinct()
        new_records_df = df.join(repli_ids_df, on="billid", how="left_anti")

        # Step 5: Insert only new records
        if new_records_df.count() > 0:
            new_records_df.select(
                "billid", "name", "origin", "desti", "billdate", "deliverydate", "ispackage"
            ).write.jdbc(
                url=jdbc_url,
                table="billrepli",
                mode="append",
                properties=properties
            )
            print(f"{new_records_df.count()} new record(s) replicated to billrepli.")
        else:
            print("No new records to replicate.")
        
        spark.stop()
    else:
        print(f"Current IST time is {current_time.strftime('%H:%M')}. Not 16:45 yet, skipping replication.")

if __name__ == "__main__":
    main()
