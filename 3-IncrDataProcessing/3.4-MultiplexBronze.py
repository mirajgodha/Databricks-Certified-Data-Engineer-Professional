# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Use the below cell contents to delete if you have ran the code previously 

# COMMAND ----------

dbutils.fs.rm(f"{dataset_bookstore}/kafka-raw/02.json")
dbutils.fs.rm(f"{dataset_bookstore}/kafka-raw/03.json")
dbutils.fs.rm(f"{dataset_bookstore}/kafka-raw/04.json")
dbutils.fs.rm(f"{dataset_bookstore}/kafka-raw/05.json")
dbutils.fs.rm(f"{dataset_bookstore}/kafka-raw/06.json")
dbutils.fs.rm(f"dbfs:/mnt/demo_pro/checkpoints/bronze", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bronze;

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                      .option("mergeSchema", True)
                      .partitionBy("topic", "year_month")
                      .trigger(availableNow=True)
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# List all active Structured Streaming queries
active_streams = spark.streams.active

if len(active_streams) == 0:
    print("✅ No active streaming queries found.")
else:
    print(f"⚙️ {len(active_streams)} active streaming queries detected:\n")

    for stream in active_streams:
        print(f"🔹 Name: {stream.name}")
        print(f"   ID: {stream.id}")
        print(f"   Is Active: {stream.isActive}")
        print(f"   Status: {stream.status['message']}")
        print(f"   Last Progress: {stream.lastProgress['batchId'] if stream.lastProgress else 'N/A'}")
        print("-" * 60)

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze
