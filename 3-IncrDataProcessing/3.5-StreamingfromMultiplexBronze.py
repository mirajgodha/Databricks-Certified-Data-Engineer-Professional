# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 CAST() Function
# MAGIC
# MAGIC In SQL (including Databricks SQL), the CAST() function converts a column or expression from one data type to another.
# MAGIC
# MAGIC ```
# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC The table bronze contains raw data — maybe from a Kafka or streaming source — where key and value are stored as binary (e.g. BINARY, VARBINARY, or BYTES type).
# MAGIC Typical in streaming pipelines:
# MAGIC
# MAGIC ```
# MAGIC key   → binary message key  
# MAGIC value → binary message payload
# MAGIC ```
# MAGIC
# MAGIC The CAST(key AS STRING) and CAST(value AS STRING) parts are converting these binary columns into human-readable text (UTF-8 string format).
# MAGIC
# MAGIC The result lets you see and interpret the Kafka messages or raw bytes as text rows in your query output.
# MAGIC
# MAGIC 🧠 Why It’s Needed
# MAGIC
# MAGIC When ingesting from Kafka or Auto Loader, data often lands like this:
# MAGIC
# MAGIC ```
# MAGIC key:   [B@3e4e9c7
# MAGIC value: [B@12f3af0
# MAGIC ```
# MAGIC
# MAGIC That’s unreadable byte data.
# MAGIC
# MAGIC So you cast it:
# MAGIC ```
# MAGIC SELECT CAST(value AS STRING)
# MAGIC ```
# MAGIC
# MAGIC → and now you can see:
# MAGIC ```
# MAGIC {"orderId":123,"amount":250,"status":"shipped"}
# MAGIC ```
# MAGIC ✅ In short
# MAGIC
# MAGIC CAST(key AS STRING) converts the raw binary data in key and value columns of the bronze table
# MAGIC into readable text strings, so you can inspect or parse the content (like JSON).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table orders_silver;

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver
