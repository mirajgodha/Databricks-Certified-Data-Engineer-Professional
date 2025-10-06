-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------


CREATE TABLE orders AS
SELECT * FROM parquet.`s3://databricks-miraj/bookstore/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As **parquet files** have well defined schema, we are able to extract the schema

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting tables
-- MAGIC 1) Paralle read queries still suceed while overwrite table is running.
-- MAGIC 2) If it fails, the old data still exits.

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`s3://databricks-miraj/bookstore/orders/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Can you se 2 table versions created.
-- MAGIC 1) CREATE TABLE AS SELECT
-- MAGIC 2) CREATE OR REPLACE TABLE AS SELECT
-- MAGIC
-- MAGIC Create or Replace always overwrite the table data.

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Insert overwrite is similar to above statement, but it will not create a table. If the schema do not match then there is a problem.
-- MAGIC So safer if the schema match is requried

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`s3://databricks-miraj/bookstore/orders/`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Schema check
-- MAGIC Can you see the error in below statement. <br><br>
-- MAGIC
-- MAGIC ```
-- MAGIC A schema mismatch detected when writing to the Delta table
-- MAGIC ```

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`s3://databricks-miraj/bookstore/orders/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`s3://databricks-miraj/bookstore/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Problem 
-- MAGIC Reexcuting the same query like insert statment has Problem. that is with insert statement it will create **duplicate** records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Databricks MERGE Operation Example
-- MAGIC
-- MAGIC This document explains how to update and insert records into a Delta table using a **MERGE** operation in Databricks.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 📘 Overview
-- MAGIC We want to:
-- MAGIC 1. Load new customer data from a JSON file in S3.
-- MAGIC 2. Create a temporary view (`customers_updates`) for that data.
-- MAGIC 3. Merge it into an existing Delta table named `customers`.
-- MAGIC
-- MAGIC This process ensures:
-- MAGIC - Existing records are updated when email info is added.
-- MAGIC - New customers are inserted automatically.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🧱 SQL Code
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Step 1: Create a temporary view from JSON data
-- MAGIC CREATE OR REPLACE TEMP VIEW customers_updates AS 
-- MAGIC SELECT * 
-- MAGIC FROM json.`s3://databricks-miraj/bookstore/customers-json-new`;
-- MAGIC
-- MAGIC -- Step 2: Merge new data into the existing 'customers' Delta table
-- MAGIC MERGE INTO customers AS c
-- MAGIC USING customers_updates AS u
-- MAGIC ON c.customer_id = u.customer_id
-- MAGIC
-- MAGIC -- Update existing rows where email was missing before
-- MAGIC WHEN MATCHED 
-- MAGIC   AND c.email IS NULL 
-- MAGIC   AND u.email IS NOT NULL THEN
-- MAGIC   UPDATE SET 
-- MAGIC     c.email = u.email,
-- MAGIC     c.updated = u.updated
-- MAGIC
-- MAGIC -- Insert new customers that don’t exist in the target table
-- MAGIC WHEN NOT MATCHED THEN 
-- MAGIC   INSERT *;
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`s3://databricks-miraj/bookstore/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "s3://databricks-miraj/bookstore/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
-- Only new records will be inserted
WHEN NOT MATCHED 
AND u.category = 'Computer Science' THEN 
  INSERT *

-- COMMAND ----------

describe books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🧾 Databricks Delta Merge Error — `_rescued_data` 
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### ❌ Error
-- MAGIC [DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve _rescued_data in INSERT clause given u.book_id, u.title, u.author, u.category, u.price.
-- MAGIC SQLSTATE: 42601
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 🧠 Root Cause
-- MAGIC This error occurs because the target Delta table (`books`) has **more columns** than the source view (`books_updates`) —  
-- MAGIC specifically, an extra internal column like `_rescued_data` exists in the target.
-- MAGIC
-- MAGIC When you use `INSERT *`, Delta tries to map **every column in the target**, but:
-- MAGIC - The `_rescued_data` column doesn’t exist in the source.
-- MAGIC - Therefore, the merge fails due to unmatched column mapping.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## ⚙️ Why `_rescued_data` Exists
-- MAGIC Databricks automatically adds a hidden column `_rescued_data` when you ingest **semi-structured data** (like CSV or JSON) using schema inference.  
-- MAGIC It stores unparseable or extra fields not matching the inferred schema.
-- MAGIC
-- MAGIC 👉 This column exists in your **`books`** Delta table, but **not** in your **`books_updates`** view.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## ✅ Fix Options
-- MAGIC
-- MAGIC ### **Option 1: Be Explicit in `INSERT` Clause (✅ Recommended)**
-- MAGIC Specify the exact columns to insert:
-- MAGIC
-- MAGIC ```sql
-- MAGIC MERGE INTO books b
-- MAGIC USING books_updates u
-- MAGIC ON b.book_id = u.book_id AND b.title = u.title
-- MAGIC WHEN NOT MATCHED 
-- MAGIC   AND u.category = 'Computer Science' THEN
-- MAGIC INSERT (book_id, title, author, category, price)
-- MAGIC VALUES (u.book_id, u.title, u.author, u.category, u.price);
-- MAGIC ```
-- MAGIC
-- MAGIC ✅ This avoids _rescued_data mapping errors and is considered best practice.
-- MAGIC
-- MAGIC Option 2: Drop _rescued_data from the Target Table (⚠️ Use with Caution)
-- MAGIC If _rescued_data was created unintentionally:
-- MAGIC
-- MAGIC ```
-- MAGIC sql
-- MAGIC Copy code
-- MAGIC ALTER TABLE books DROP COLUMN _rescued_data;
-- MAGIC ```
-- MAGIC
-- MAGIC Then rerun your MERGE statement.
-- MAGIC
-- MAGIC ⚠️ Only perform this if you are sure _rescued_data is not required for tracking malformed or extra fields.

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED 
  AND u.category = 'Computer Science' THEN
INSERT (book_id, title, author, category, price)
VALUES (u.book_id, u.title, u.author, u.category, u.price);
