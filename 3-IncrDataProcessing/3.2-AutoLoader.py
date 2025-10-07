# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source Directory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC **🔹 Auto Loader**
# MAGIC
# MAGIC A Databricks feature that incrementally loads new files from cloud storage (S3, ADLS, GCS, or DBFS) using optimized metadata tracking.
# MAGIC
# MAGIC **🔹 Schema Evolution**
# MAGIC
# MAGIC Auto Loader automatically detects and handles changes in schema without requiring manual adjustments.
# MAGIC
# MAGIC **🔹 Checkpointing**
# MAGIC
# MAGIC Ensures fault tolerance and exactly-once semantics by saving metadata and progress.
# MAGIC
# MAGIC **🔹 Delta Table**
# MAGIC
# MAGIC The target table format supporting ACID operations, versioning, and efficient incremental writes — ideal for streaming workloads.
# MAGIC
# MAGIC ## Code Explanation: Spark Structured Streaming with Auto Loader
# MAGIC
# MAGIC ```python
# MAGIC spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "parquet")
# MAGIC     .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
# MAGIC     .load(f"{dataset_bookstore}/orders-raw")
# MAGIC   .writeStream
# MAGIC     .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
# MAGIC     .table("orders_updates")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### **Step-by-Step Explanation**
# MAGIC
# MAGIC 1. **Reading Data as a Stream (`readStream`)**
# MAGIC     - `spark.readStream` initiates a streaming read, meaning Spark continuously monitors a directory or source for new data files as they arrive.
# MAGIC 2. **Using Auto Loader (`format("cloudFiles")`)**
# MAGIC     - Setting the format to `cloudFiles` activates Databricks Auto Loader. This is a continuous file ingestion tool that efficiently tracks new files in cloud storage (like S3, ADLS, or DBFS) and reads them as they arrive.[^2]
# MAGIC 3. **Specifying Data Format (`cloudFiles.format`)**
# MAGIC     - Option `cloudFiles.format` set to `parquet` tells Auto Loader to expect **Parquet files** in the streaming source directory.
# MAGIC 4. **Schema Location for Auto Loader (`cloudFiles.schemaLocation`)**
# MAGIC     - `cloudFiles.schemaLocation` tells Auto Loader where to store metadata about the schema it infers. This location should be a persistent DBFS or cloud path (here: `dbfs:/mnt/demo/orders_checkpoint`).
# MAGIC 5. **Loading the Data (`load(...)`)**
# MAGIC     - Loads streaming data from the directory specified by `dataset_bookstore + '/orders-raw'` — Spark will continuously monitor this folder for new Parquet files and process them as they arrive.
# MAGIC 6. **Writing the Stream (`writeStream`)**
# MAGIC     - Now we define the streaming sink — what to do with the data as it is read in. `writeStream` defines the action.
# MAGIC 7. **Checkpointing (`checkpointLocation`)**
# MAGIC     - Checkpoints (here at the same path as schema location) store progress and state information, allowing Spark to recover smoothly from failures or restarts. Each stream should have a unique checkpoint location.[^2]
# MAGIC 8. **Writing to a Table (`table(...)`)**
# MAGIC     - `table("orders_updates")` writes the continuous stream directly into a Delta table called `orders_updates`. This means any records arriving in the source directory are immediately available for querying via this table (in near real-time).
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### **Summary Table**
# MAGIC
# MAGIC | Stage | Purpose |
# MAGIC | :-- | :-- |
# MAGIC | `.readStream` | Reads data as a live stream—monitors the directory continually |
# MAGIC | `format("cloudFiles")` | Uses Databricks Auto Loader for efficient file ingestion |
# MAGIC | `.option("cloudFiles.format", "parquet")` | Ingests files in Parquet format |
# MAGIC | `.option("cloudFiles.schemaLocation", ... )` | Saves schema metadata for evolving datasets |
# MAGIC | `.load(source)` | Source directory for streaming data |
# MAGIC | `.writeStream` | Defines the streaming sink (where the data goes) |
# MAGIC | `.option("checkpointLocation", ...)` | Tracks progress and ensures recovery on failure |
# MAGIC | `.table("orders_updates")` | Writes the data stream into a Delta table for downstream queries |
# MAGIC
# MAGIC ### **Why Use This Pattern?**
# MAGIC
# MAGIC - **Auto Loader**: Handles schema inference, new file tracking, and efficient scaling.[^7][^2]
# MAGIC - **Checkpointing**: Ensures fault tolerance; you never lose track of streaming progress.
# MAGIC - **Direct table output**: Makes new data instantly accessible in Databricks SQL, notebooks, and dashboards.
# MAGIC
# MAGIC For further reading, see the [Databricks Structured Streaming tutorial] or the [official Spark Structured Streaming guide].[^4][^6][^2]
# MAGIC <span style="display:none">[^1][^3][^5][^8]</span>
# MAGIC
# MAGIC <div align="center">⁂</div>
# MAGIC
# MAGIC [^1]: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
# MAGIC
# MAGIC [^2]: https://docs.databricks.com/aws/en/structured-streaming/tutorial
# MAGIC
# MAGIC [^3]: https://community.databricks.com/t5/data-engineering/how-to-make-structured-streaming-with-autoloader-efficiently-and/td-p/47833
# MAGIC
# MAGIC [^4]: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/tutorial
# MAGIC
# MAGIC [^5]: https://stackoverflow.com/questions/75816983/spark-stream-handling-of-folders-with-different-file-formats
# MAGIC
# MAGIC [^6]: https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html
# MAGIC
# MAGIC [^7]: https://hevodata.com/learn/databricks-autoloader/
# MAGIC
# MAGIC [^8]: https://www.softserveinc.com/en-us/blog/optimizing-spark-structured-streaming
# MAGIC
# MAGIC

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Landing New Files

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
