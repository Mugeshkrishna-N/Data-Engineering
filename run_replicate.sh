#!/bin/bash

# Use spark-submit from Homebrew installation
SPARK_SUBMIT="/opt/homebrew/bin/spark-submit"

# JDBC JAR file
JDBC_JAR="/Users/mugesh_krishna/Downloads/mysql-connector-j-9.3.0/mysql-connector-j-9.3.0.jar"

# Path to your PySpark script
PYSPARK_SCRIPT="/Users/mugesh_krishna/projects/autorepli.py"

# Log file location
LOG_FILE="/Users/mugesh_krishna/projects/replicate.log"

# Timestamped log header
echo "----- Running replication at $(date) -----" >> "$LOG_FILE"

# Run the PySpark job
"$SPARK_SUBMIT" \
  --jars "$JDBC_JAR" \
  "$PYSPARK_SCRIPT" >> "$LOG_FILE" 2>&1

echo "----- Finished replication at $(date) -----" >> "$LOG_FILE"

