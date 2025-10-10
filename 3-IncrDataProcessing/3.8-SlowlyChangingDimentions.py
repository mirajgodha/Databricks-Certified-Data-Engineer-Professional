# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/mirajgodha/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/scd2.png" alt="Flow Diagram" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type 2 Slowly Changing Dimension (SCD)
# MAGIC This pipeline performs a **Type 2 Upsert (SCD Type 2)** on a books_silver Delta table using streaming data from the Bronze layer.
# MAGIC
# MAGIC That means:
# MAGIC
# MAGIC If a book’s price changes → close (expire) the old record
# MAGIC
# MAGIC Insert a new record as the latest version (current = true)
# MAGIC
# MAGIC Keep history of all changes
# MAGIC
# MAGIC ---
# MAGIC ## **What is Type 2 SCD?**
# MAGIC
# MAGIC Type 2 SCD maintains **full historical data** by creating a new record for each change instead of overwriting existing data. It uses flags and timestamps to track current vs historical records.[^1][^2][^3]
# MAGIC
# MAGIC ## **Code Explanation**
# MAGIC
# MAGIC ### **1. Table Structure**
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver (
# MAGIC     book_id STRING,
# MAGIC     title STRING, 
# MAGIC     author STRING,
# MAGIC     price DOUBLE,
# MAGIC     current BOOLEAN,        -- Flag to identify active records
# MAGIC     effective_date TIMESTAMP,  -- When this version became active
# MAGIC     end_date TIMESTAMP         -- When this version expired (NULL for current)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### **2. The `type2_upsert` Function**
# MAGIC
# MAGIC This function processes each micro-batch of streaming data:
# MAGIC
# MAGIC ```python
# MAGIC def type2_upsert(microBatchDF, batch):
# MAGIC     microBatchDF.createOrReplaceTempView("updates")
# MAGIC ```
# MAGIC
# MAGIC **Creates a temporary view** from incoming data for SQL processing.
# MAGIC
# MAGIC ### **3. Complex MERGE Logic**
# MAGIC
# MAGIC The SQL query handles two scenarios in the `USING` clause:
# MAGIC
# MAGIC #### **Scenario A: Direct Updates**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT updates.book_id as merge_key, updates.* 
# MAGIC FROM updates
# MAGIC ```
# MAGIC
# MAGIC Handles **new books** or **price changes**.
# MAGIC
# MAGIC #### **Scenario B: Historical Record Closure**
# MAGIC
# MAGIC ```sql
# MAGIC SELECT NULL as merge_key, updates.* 
# MAGIC FROM updates 
# MAGIC JOIN books_silver ON updates.book_id = books_silver.book_id 
# MAGIC WHERE books_silver.current = true AND updates.price <> books_silver.price
# MAGIC ```
# MAGIC
# MAGIC Identifies existing records that need to be **marked as historical** when price changes.
# MAGIC
# MAGIC ### **4. MERGE Actions**
# MAGIC
# MAGIC #### **WHEN MATCHED** (Price Changed)
# MAGIC
# MAGIC ```sql
# MAGIC WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price 
# MAGIC THEN UPDATE SET current = false, end_date = staged_updates.updated
# MAGIC ```
# MAGIC
# MAGIC **Closes the historical record** by setting `current = false` and adding an `end_date`.
# MAGIC
# MAGIC #### **WHEN NOT MATCHED** (New Record)
# MAGIC
# MAGIC ```sql
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (book_id, title, author, price, current, effective_date, end_date) 
# MAGIC VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, 
# MAGIC         staged_updates.price, true, staged_updates.updated, NULL)
# MAGIC ```
# MAGIC
# MAGIC **Inserts new current record** with `current = true` and `end_date = NULL`.
# MAGIC
# MAGIC ### **5. Streaming Pipeline**
# MAGIC
# MAGIC ```python
# MAGIC def process_books():
# MAGIC     query = (spark.readStream
# MAGIC              .table("bronze")
# MAGIC              .filter("topic = 'books'")
# MAGIC              .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
# MAGIC              .select("v.*")
# MAGIC              .writeStream
# MAGIC              .foreachBatch(type2_upsert)  # Apply SCD logic to each batch
# MAGIC              .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")
# MAGIC              .trigger(availableNow=True)  # Process all available data once
# MAGIC              .start())
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ## **How It Works: Example**
# MAGIC
# MAGIC ### **Initial State:**
# MAGIC
# MAGIC | book_id | title | price | current | effective_date | end_date |
# MAGIC | :-- | :-- | :-- | :-- | :-- | :-- |
# MAGIC | B001 | "Spark Guide" | 29.99 | true | 2024-01-01 | NULL |
# MAGIC
# MAGIC ### **Price Update (29.99 → 34.99):**
# MAGIC
# MAGIC **Step 1:** Historical record is closed:
# MAGIC
# MAGIC
# MAGIC | book_id | title | price | current | effective_date | end_date |
# MAGIC | :-- | :-- | :-- | :-- | :-- | :-- |
# MAGIC | B001 | "Spark Guide" | 29.99 | **false** | 2024-01-01 | **2024-01-15** |
# MAGIC
# MAGIC **Step 2:** New current record is inserted:
# MAGIC
# MAGIC
# MAGIC | book_id | title | price | current | effective_date | end_date |
# MAGIC | :-- | :-- | :-- | :-- | :-- | :-- |
# MAGIC | B001 | "Spark Guide" | 29.99 | false | 2024-01-01 | 2024-01-15 |
# MAGIC | B001 | "Spark Guide" | **34.99** | **true** | **2024-01-15** | **NULL** |
# MAGIC
# MAGIC ## **Key Benefits**
# MAGIC
# MAGIC - **Historical Tracking**: Complete audit trail of all price changes[^3][^1]
# MAGIC - **Point-in-Time Analysis**: Query data as it existed at any date[^2][^4]
# MAGIC - **Data Integrity**: No data loss, only additions[^5][^6]
# MAGIC - **Business Intelligence**: Enables trend analysis and historical reporting[^7][^8]
# MAGIC
# MAGIC
# MAGIC ## **Summary**
# MAGIC
# MAGIC This implementation provides a **production-ready Type 2 SCD pattern** that:
# MAGIC
# MAGIC 1. **Preserves all historical data**
# MAGIC 2. **Maintains current record flags**
# MAGIC 3. **Uses effective date ranges**
# MAGIC 4. **Processes streaming updates** in real-time
# MAGIC 5. **Handles complex change scenarios** automatically
# MAGIC
# MAGIC The pattern is commonly used in data warehouses and data lakes for maintaining dimensional data that changes over time while preserving complete history.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NOTE: This cell contains the SQL query for your reference, and won't work if run directly.
# MAGIC -- The query is used below in the type2_upsert() function as part of the foreachBatch call.
# MAGIC
# MAGIC MERGE INTO books_silver
# MAGIC USING (
# MAGIC     SELECT updates.book_id as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT NULL as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC     JOIN books_silver ON updates.book_id = books_silver.book_id
# MAGIC     WHERE books_silver.current = true AND updates.price <> books_silver.price
# MAGIC   ) staged_updates
# MAGIC ON books_silver.book_id = merge_key 
# MAGIC WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
# MAGIC   UPDATE SET current = false, end_date = staged_updates.updated
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (book_id, title, author, price, current, effective_date, end_date)
# MAGIC   VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)

# COMMAND ----------

def type2_upsert(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("updates")
    
    sql_query = """
        MERGE INTO books_silver
        USING (
            SELECT updates.book_id as merge_key, updates.*
            FROM updates

            UNION ALL

            SELECT NULL as merge_key, updates.*
            FROM updates
            JOIN books_silver ON updates.book_id = books_silver.book_id
            WHERE books_silver.current = true AND updates.price <> books_silver.price
          ) staged_updates
        ON books_silver.book_id = merge_key 
        WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
          UPDATE SET current = false, end_date = staged_updates.updated
        WHEN NOT MATCHED THEN
          INSERT (book_id, title, author, price, current, effective_date, end_date)
          VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
    """
    
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

def process_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
 
    query = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'books'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                 .writeStream
                    .foreachBatch(type2_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/current_books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_books
# MAGIC AS SELECT book_id, title, author, price
# MAGIC    FROM books_silver
# MAGIC    WHERE current IS TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_books
# MAGIC ORDER BY book_id
