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
# MAGIC ## Reading Stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Stream
# MAGIC <br><br>
# MAGIC ```
# MAGIC  spark.readStream
# MAGIC
# MAGIC       .table("books")
# MAGIC       .createOrReplaceTempView("books_streaming_tmp_vw")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC This code creates a temporary view from a streaming DataFrame, allowing SQL queries on continuously arriving data from the "books" table.
# MAGIC
# MAGIC ### Step-by-Step Explanation
# MAGIC
# MAGIC - `(spark.readStream.table("books"))`: Reads the "books" table as a **structured streaming source**. This means the DataFrame will update continuously as new data is added to the table.[^1][^3][^8]
# MAGIC - `.createOrReplaceTempView("books_streaming_tmp_vw")`: Registers the streaming DataFrame as a **temporary view** named "books_streaming_tmp_vw". This lets you query the latest records using Databricks SQL statements such as `SELECT * FROM books_streaming_tmp_vw`.
# MAGIC
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC - **Structured Streaming**: Spark's API for working with real-time streams of data, incrementally processing new records as they arrive.
# MAGIC - **Delta Table Streaming**: In Databricks, you can stream changes from Delta tables; new rows added to the "books" table become part of the DataFrame automatically.
# MAGIC - **Temporary View**: A view defined in a Spark session that can be queried using SQL, including continuous queries on incoming streaming data.
# MAGIC
# MAGIC
# MAGIC ### Typical Use Case
# MAGIC
# MAGIC - After executing this code, SQL queries like `SELECT title, author FROM books_streaming_tmp_vw WHERE price > 100;` will always operate on up-to-date, streaming data.
# MAGIC
# MAGIC
# MAGIC ### Summary Table
# MAGIC
# MAGIC | Step | Purpose |
# MAGIC | :-- | :-- |
# MAGIC | `.readStream.table("books")` | Create a streaming DataFrame from "books" |
# MAGIC | `.createOrReplaceTempView(...)` | Make it queryable as a temp SQL view |
# MAGIC
# MAGIC This is a common pattern for combining Spark Structured Streaming with SQL-based analytics on real-time data in Databricks.[^3][^8][^1]
# MAGIC <span style="display:none">[^2][^4][^5][^6][^7]</span>
# MAGIC
# MAGIC <div align="center">⁂</div>
# MAGIC
# MAGIC [^1]: https://docs.databricks.com/aws/en/structured-streaming/delta-lake
# MAGIC
# MAGIC [^2]: https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html
# MAGIC
# MAGIC [^3]: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/delta-lake
# MAGIC
# MAGIC [^4]: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
# MAGIC
# MAGIC [^5]: https://docs.databricks.com/aws/en/dlt/streaming-tables
# MAGIC
# MAGIC [^6]: https://delta-docs-incubator.netlify.app/delta-streaming/
# MAGIC
# MAGIC [^7]: https://www.macrometa.com/event-stream-processing/spark-structured-streaming
# MAGIC
# MAGIC [^8]: https://stackoverflow.com/questions/76599437/in-pyspark-what-is-the-difference-between-spark-read-and-spark-readstream
# MAGIC
# MAGIC

# COMMAND ----------

(spark.readStream
      .table("books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

display(
  spark.table("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

display(
  spark.sql("""
    SELECT author, count(book_id) AS total_books
    FROM books_streaming_tmp_vw
    GROUP BY author
  """),
  checkpointLocation="s3://databricks-miraj/checkpoint1/"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %md
# MAGIC Order by is unsupported without watermarking in streaming opertions

# COMMAND ----------

display(
  spark.sql("""
 SELECT * 
 FROM books_streaming_tmp_vw
 ORDER BY author"""),
  checkpointLocation="s3://databricks-miraj/checkpoint1/"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Spark Structured Streaming: Explanation of the Code
# MAGIC
# MAGIC ```python
# MAGIC (
# MAGIC   spark.table("author_counts_tmp_vw")                               
# MAGIC       .writeStream  
# MAGIC       .trigger(processingTime='4 seconds')
# MAGIC       .outputMode("complete")
# MAGIC       .option("checkpointLocation", "s3://databricks-miraj/checkpoint3/")
# MAGIC       .table("author_counts")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### **Step-by-Step Breakdown**
# MAGIC
# MAGIC #### **1. Source Data Table**
# MAGIC
# MAGIC - **`spark.table("author_counts_tmp_vw")`**
# MAGIC     - Reads data from a temporary or intermediate Spark SQL table/view named `author_counts_tmp_vw`.
# MAGIC     - Typically, this view contains aggregated/streaming results (such as running counts by author).
# MAGIC
# MAGIC
# MAGIC #### **2. Start a Streaming Write**
# MAGIC
# MAGIC - **`.writeStream`**
# MAGIC     - Activates streaming write mode so the results of the ongoing streaming or incremental computation can be written continuously.
# MAGIC
# MAGIC
# MAGIC #### **3. Processing Trigger**
# MAGIC
# MAGIC - **`.trigger(processingTime='4 seconds')`**
# MAGIC     - Defines how often (every 4 seconds) Spark should process available new data and update the output.
# MAGIC     - This ensures near real-time updates for the downstream table.
# MAGIC
# MAGIC
# MAGIC #### **4. Output Mode**
# MAGIC
# MAGIC - **`.outputMode("complete")`**
# MAGIC     - The entire result table is re-written and updated at each trigger (every 4 seconds).
# MAGIC     - **Common for aggregating streaming queries**—each trigger emits the full aggregation result, not just new or changed rows.
# MAGIC
# MAGIC
# MAGIC #### **5. Checkpointing**
# MAGIC
# MAGIC - **`.option("checkpointLocation", "s3://databricks-miraj/checkpoint3/")`**
# MAGIC     - Specifies where Spark should store progress, state, and fault-tolerance information.
# MAGIC     - **Required for reliability**: this allows recovery (resume without data loss) after driver or node failures.
# MAGIC
# MAGIC
# MAGIC #### **6. Write to a Table**
# MAGIC
# MAGIC - **`.table("author_counts")`**
# MAGIC     - Continuously writes the processed/aggregated output into a managed Delta table called `author_counts`.
# MAGIC     - The results in this table are always kept up to date; this table can be queried for the latest author aggregation at any time.
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### **Summary Table**
# MAGIC
# MAGIC | Code Section | Purpose / Description |
# MAGIC | :-- | :-- |
# MAGIC | `spark.table("author_counts_tmp_vw")` | Reads data from the temporary/aggregated input view |
# MAGIC | `.writeStream` | Sets up output as a continuous, streaming operation |
# MAGIC | `.trigger(processingTime='4 seconds')` | Runs streaming query every 4 seconds |
# MAGIC | `.outputMode("complete")` | Every trigger, replace all rows in the output table with new aggregation |
# MAGIC | `.option("checkpointLocation", "...")` | Stores checkpoint (progress, lineage, state, recovery info) |
# MAGIC | `.table("author_counts")` | Writes the streaming aggregation output into the managed Delta table |
# MAGIC
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### **When is this Pattern Used?**
# MAGIC
# MAGIC - When you want a table (like `author_counts`) to always display the latest aggregate or summary from an ongoing data stream.
# MAGIC - Especially common in dashboards and monitoring applications, or when external systems need up-to-the-moment analytics from streaming data.
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC **In summary**: This code sets up a continuous streaming pipeline where every 4 seconds, Spark refreshes the entire aggregation result and writes it to a Delta table, while maintaining recoverability and exactly-once guarantees via checkpointing.
# MAGIC <span style="display:none">[^1][^2][^3][^4][^5][^6][^7][^8][^9]</span>
# MAGIC
# MAGIC <div align="center">⁂</div>
# MAGIC
# MAGIC [^1]: https://docs.databricks.com/aws/en/structured-streaming/delta-lake
# MAGIC
# MAGIC [^2]: https://docs.databricks.com/aws/en/structured-streaming/examples
# MAGIC
# MAGIC [^3]: https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring
# MAGIC
# MAGIC [^4]: https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html
# MAGIC
# MAGIC [^5]: https://docs.databricks.com/aws/en/structured-streaming/tutorial
# MAGIC
# MAGIC [^6]: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/examples
# MAGIC
# MAGIC [^7]: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
# MAGIC
# MAGIC [^8]: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/output-mode
# MAGIC
# MAGIC [^9]: https://www.softserveinc.com/en-us/blog/optimizing-spark-structured-streaming
# MAGIC
# MAGIC

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream  
      .trigger(processingTime='4 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "s3://databricks-miraj/checkpoint3/")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B191", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ### availableNow=True
# MAGIC <br><br>
# MAGIC
# MAGIC ```
# MAGIC (spark.table("author_counts_tmp_vw")                               
# MAGIC       .writeStream           
# MAGIC       .trigger(availableNow=True)
# MAGIC       .outputMode("complete")
# MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
# MAGIC       .table("author_counts")
# MAGIC       .awaitTermination()
# MAGIC )
# MAGIC ```
# MAGIC #### ⚙️ Step-by-Step Explanation
# MAGIC 1. spark.table("author_counts_tmp_vw")
# MAGIC
# MAGIC Reads the temporary view author_counts_tmp_vw as a streaming DataFrame.
# MAGIC
# MAGIC The view might come from an earlier aggregation or transformation.
# MAGIC
# MAGIC 2. .writeStream
# MAGIC
# MAGIC Converts the DataFrame into a streaming sink, allowing continuous or triggered output.
# MAGIC
# MAGIC This starts the write phase of the structured streaming pipeline.
# MAGIC
# MAGIC 3. **.trigger(availableNow=True)**
# MAGIC
# MAGIC Executes the stream once to process all data currently available, then **stops automatically.**
# MAGIC
# MAGIC Acts like a batch job over streaming data.
# MAGIC
# MAGIC Ideal for incremental ETL jobs or scheduled data updates.
# MAGIC
# MAGIC 4. .outputMode("complete")
# MAGIC
# MAGIC Writes the entire result table on every trigger.
# MAGIC
# MAGIC Often used when the query involves aggregations (like counts or sums).
# MAGIC
# MAGIC ⚠️ Note: Can be expensive for large datasets since it rewrites the full result each time.
# MAGIC
# MAGIC 5. .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
# MAGIC
# MAGIC Defines where Spark stores checkpoint data (progress, offsets, and state).
# MAGIC
# MAGIC Ensures fault tolerance — the stream can recover from failures or restarts.
# MAGIC
# MAGIC 6. .table("author_counts")
# MAGIC
# MAGIC Writes the streaming results to a Delta table named author_counts.
# MAGIC
# MAGIC Delta ensures ACID transactions, versioning, and efficient incremental writes.
# MAGIC
# MAGIC 7. .awaitTermination()
# MAGIC
# MAGIC Keeps the stream active until it finishes or is manually stopped.
# MAGIC
# MAGIC With availableNow=True, the query terminates automatically after processing all pending data.

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts
