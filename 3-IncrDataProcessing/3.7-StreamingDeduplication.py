# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check duplicate records in our data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch - removing duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Parsing and Counting Unique Orders from JSON in a Table
# MAGIC
# MAGIC Let's break down the provided PySpark code and explain each step:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"
# MAGIC
# MAGIC batch_total = (
# MAGIC   spark.read
# MAGIC     .table("bronze")
# MAGIC     .filter("topic = 'orders'")
# MAGIC     .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
# MAGIC     .select("v.*")
# MAGIC     .dropDuplicates(["order_id", "order_timestamp"])
# MAGIC     .count()
# MAGIC )
# MAGIC
# MAGIC print(batch_total)
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC #### **What does this code do?**
# MAGIC
# MAGIC This code reads data from a table containing raw JSON records, parses the JSON to structured columns, removes duplicate orders (by `order_id` and `order_timestamp`), and then counts the number of unique orders.
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC #### **Step-by-Step Breakdown**
# MAGIC
# MAGIC 1. **Importing needed functions**
# MAGIC     - `from pyspark.sql import functions as F`: Imports Spark SQL functions for transformations.
# MAGIC 2. **Defining the JSON schema**
# MAGIC     - `json_schema = ...`: Specifies the expected structure of the JSON data as a schema string (so Spark knows how to parse the JSON string into columns and nested fields).
# MAGIC 3. **Reading the Bronze Table**
# MAGIC     - `spark.read.table("bronze")`: Reads the 'bronze' table, which typically stores ingestion-stage, unparsed/raw data.
# MAGIC 4. **Filtering for 'orders' records**
# MAGIC     - `.filter("topic = 'orders'")`: Keeps only records where the topic column equals 'orders'.
# MAGIC 5. **Parsing JSON in the 'value' column**
# MAGIC     - `.select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))`: Converts the binary or string column 'value' (which contains JSON data) into a structured set of columns according to `json_schema`. The result is a single struct column called 'v'.
# MAGIC 6. **Expanding the parsed columns**
# MAGIC     - `.select("v.*")`: Flattens the struct 'v' back into regular DataFrame columns, so each field (like `order_id`, `total`, `books`, etc) becomes a top-level column.
# MAGIC 7. **Removing duplicates**
# MAGIC     - `.dropDuplicates(["order_id", "order_timestamp"])`: Keeps only one record for each unique (`order_id`, `order_timestamp`) pair, dropping any repeated orders.
# MAGIC 8. **Counting unique orders**
# MAGIC     - `.count()`: Returns the total number of unique orders after parsing and deduplication.
# MAGIC 9. **Printing the count**
# MAGIC     - `print(batch_total)`: Prints the resulting number to standard output.
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC #### **Key Concepts Demonstrated**
# MAGIC
# MAGIC - **from_json**: Parses a column of JSON strings to typed columns according to a user-defined schema.
# MAGIC - **dropDuplicates**: Removes duplicate rows based on specific columns.
# MAGIC - **Working with Nested JSON**: The `books` field is an array of structs, showing how Spark handles nested arrays/objects from JSON.
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC #### **Summary Table**
# MAGIC
# MAGIC | Step | Purpose |
# MAGIC | :-- | :-- |
# MAGIC | Import functions | Use Spark transformation utilities |
# MAGIC | Define schema | Declare how to interpret/restructure the JSON |
# MAGIC | Read staging table | Load earliest (bronze) data |
# MAGIC | Filter by topic | Only process order records |
# MAGIC | Parse JSON to struct | Convert JSON blobs to typed Spark DataFrame columns |
# MAGIC | Flatten columns | Make JSON fields accessible as standard columns |
# MAGIC | Remove duplicates | Only count each unique order once |
# MAGIC | Count result | Total number of unique, valid orders in this batch |
# MAGIC
# MAGIC **By using `from_json` and a well-defined schema, Spark turns raw JSON text into easily usable, typed columns for analysis.**
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                )

print(batch_total)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming - Removing Duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Watermarking
# MAGIC
# MAGIC Watermarking tells Spark how long to wait for late data before considering a given time window “complete” and safe to drop from memory (state).
# MAGIC
# MAGIC **In other words —**
# MAGIC
# MAGIC It defines the maximum allowed delay for late-arriving events in event-time processing.
# MAGIC
# MAGIC **🧩 Why It’s Needed**
# MAGIC
# MAGIC - In streaming, data doesn’t always arrive in perfect order:
# MAGIC - Some events might arrive late (e.g., network delay, Kafka lag).
# MAGIC - Spark keeps an in-memory state for windows (like 5-minute aggregations).
# MAGIC - Without watermarking, Spark would keep state forever → leading to unbounded memory growth.
# MAGIC - Watermarking fixes that by cleaning up old state after a defined delay.
# MAGIC
# MAGIC <br><br>
# MAGIC
# MAGIC ```python
# MAGIC deduped_df = (spark.readStream
# MAGIC                    .table("bronze")
# MAGIC                    .filter("topic = 'orders'")
# MAGIC                    .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
# MAGIC                    .select("v.*")
# MAGIC                    .withWatermark("order_timestamp", "30 seconds")
# MAGIC                    .dropDuplicates(["order_id", "order_timestamp"]))
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC 1. **`spark.readStream.table("bronze")`**: Reads data as a streaming DataFrame from the "bronze" table, which typically stores raw ingestion data.
# MAGIC 2. **`.filter("topic = 'orders'")`**: Filters to only include rows where the `topic` column equals `'orders'`, focusing the stream on order events.
# MAGIC 3. **`.select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))`**: Parses the JSON string in the `value` column (cast to string) into a structured column `v` according to the defined `json_schema`.
# MAGIC 4. **`.select("v.*")`**: Expands the fields of the parsed JSON struct `v` into top-level columns for easy access.
# MAGIC 5. **`.withWatermark("order_timestamp", "30 seconds")`**: Sets a watermark on the `order_timestamp` column with a 30-second delay. This informs Spark to consider data delayed by more than 30 seconds as late and discard it for stateful operations like deduplication.
# MAGIC 6. **`.dropDuplicates(["order_id", "order_timestamp"])`**: Performs deduplication on the streaming data based on the combination of `order_id` and `order_timestamp` columns, ensuring that duplicate orders are removed within the watermark window.
# MAGIC
# MAGIC ### Key Concept: Watermark and Deduplication
# MAGIC
# MAGIC - **Watermarking** limits the state that Spark maintains by defining how late data can arrive (30 seconds here).
# MAGIC - **Deduplication with watermark** enables efficient state management by dropping duplicates within this time window.
# MAGIC
# MAGIC
# MAGIC ### Behavior:
# MAGIC
# MAGIC - Holds state to track seen `(order_id, order_timestamp)` pairs within the watermark.
# MAGIC - Removes duplicate events arriving within 30 seconds delay of event time.
# MAGIC - Events arriving later than 30 seconds after watermark advance are considered late and ignored.
# MAGIC
# MAGIC
# MAGIC ### Relevant Insights from Documentation and Community Resources:
# MAGIC
# MAGIC - Deduplication state store grows with the number of unique keys but is bounded by the watermark delay.
# MAGIC - Using `dropDuplicates` alone can cause unbounded state build-up, but combined with watermark it helps prune state for late data.
# MAGIC - Spark writes new events immediately as they arrive; watermarking affects how late events and state expiration are handled.
# MAGIC
# MAGIC
# MAGIC ### Summary Table
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC | :-- | :-- |
# MAGIC | `readStream.table()` | Streaming source reading raw ingestion data |
# MAGIC | `filter()` | Filter to relevant topic ('orders') |
# MAGIC | `from_json()` | Parse JSON from string to structured columns |
# MAGIC | `withWatermark()` | Define event-time delay for late data |
# MAGIC | `dropDuplicates()` | Remove duplicate rows based on order ID and timestamp |
# MAGIC
# MAGIC This pattern is essential for processing event streams with possible duplicate events and late arrivals, enabling accurate, real-time, and memory-efficient stream analytics.
# MAGIC

# COMMAND ----------

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

# MAGIC %md
# MAGIC We also have to ensure that insrted records are not in the table, otherwise deduplication will be of no use.

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table orders_silver;

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)
    #microBatchDF._jdf.sparkSession().sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

# MAGIC %md
# MAGIC ### foreachBatch
# MAGIC
# MAGIC ```python
# MAGIC query = (deduped_df.writeStream
# MAGIC              .foreachBatch(upsert_data)
# MAGIC              .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
# MAGIC              .trigger(availableNow=True)
# MAGIC              .start())
# MAGIC
# MAGIC query.awaitTermination()
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### What does this do?
# MAGIC
# MAGIC 1. **`deduped_df.writeStream`**
# MAGIC Starts defining a streaming write operation on the streaming DataFrame `deduped_df`.
# MAGIC 2. **`.foreachBatch(upsert_data)`**
# MAGIC For each micro-batch of data processed by the streaming query, the function `upsert_data` is called with two arguments:
# MAGIC     - The micro-batch as a regular (batch) DataFrame
# MAGIC     - The unique batch ID
# MAGIC
# MAGIC This allows you to execute arbitrary batch logic, such as writing the micro-batch to a Delta table using `MERGE INTO` for UPSERT operations, or writing to data sinks without native streaming support.
# MAGIC 3. **`.option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")`**
# MAGIC Sets the checkpoint directory used by Spark to save streaming progress and metadata for fault tolerance. It's important for exactly-once processing and recovery.
# MAGIC 4. **`.trigger(availableNow=True)`**
# MAGIC Runs the streaming query once immediately processing all available data, then stops. This is useful for backfill or one-off processing using streaming mechanisms.
# MAGIC 5. **`.start()`**
# MAGIC Starts the execution of the streaming query asynchronously.
# MAGIC 6. **`query.awaitTermination()`**
# MAGIC Blocks the driver program until the streaming query `query` finishes (which it will because of `availableNow=True` trigger).
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### Typical Usage Scenario
# MAGIC
# MAGIC - You have stateful streaming data (`deduped_df`).
# MAGIC - For each micro-batch, you want to apply complex batch logic (upserts, joins, non-native sinks).
# MAGIC - Using `foreachBatch`, you receive batch-like DataFrames for ease of handling.
# MAGIC - Ideal for:
# MAGIC     - Writing Delta Lake merges for upsert semantics
# MAGIC     - Writing to external databases without native streaming support
# MAGIC     - Performing batch joins or transformations that are difficult in streaming APIs
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### Important Details about `.foreachBatch`
# MAGIC
# MAGIC - **Works only in micro-batch mode** (not continuous processing mode).
# MAGIC - Guarantees **at-least-once semantics**; you need your batch function (e.g., upsert) to be idempotent to achieve exactly-once behavior.
# MAGIC - Allows access to classic DataFrame API inside the batch function for maximum flexibility.
# MAGIC - Must **fully consume and handle** the batch DataFrame inside the function to avoid streaming query hangs.
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### Example of `upsert_data` function (simplified):
# MAGIC
# MAGIC ```python
# MAGIC def upsert_data(batch_df, batch_id):
# MAGIC     batch_df.createOrReplaceTempView("batch_data")
# MAGIC     spark.sql("""
# MAGIC       MERGE INTO silver_orders AS target
# MAGIC       USING batch_data AS source
# MAGIC       ON target.order_id = source.order_id
# MAGIC       WHEN MATCHED THEN UPDATE SET *
# MAGIC       WHEN NOT MATCHED THEN INSERT *
# MAGIC     """)
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC ### Summary Table
# MAGIC
# MAGIC | Component | Description |
# MAGIC | :-- | :-- |
# MAGIC | `writeStream` | Start streaming output definition |
# MAGIC | `foreachBatch` | Apply function to each micro-batch of data |
# MAGIC | `checkpointLocation` | Store streaming state and progress to allow recovery |
# MAGIC | `trigger(availableNow=True)` | Process all available data once and stop |
# MAGIC | `start()` | Begin streaming execution asynchronously |
# MAGIC | `awaitTermination()` | Block until streaming query completes |
# MAGIC
# MAGIC
# MAGIC ***
# MAGIC
# MAGIC This pattern provides powerful flexibility to handle complex batch operations within a streaming pipeline, combining streaming scale and batch capabilities into one.
# MAGIC
# MAGIC References:
# MAGIC
# MAGIC - [Databricks foreachBatch docs](https://docs.databricks.com/en/structured-streaming/foreach.html)
# MAGIC - [Apache Spark foreachBatch API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html)

# COMMAND ----------

# MAGIC %md
# MAGIC Since we have used .trigger(availableNow=True), this query will:
# MAGIC
# MAGIC Process all available data once,
# MAGIC
# MAGIC Then stop automatically (like a batch job).

# COMMAND ----------

query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver2")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC So we can see the records are mached in our batch query and output of streaming query as well.

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")
