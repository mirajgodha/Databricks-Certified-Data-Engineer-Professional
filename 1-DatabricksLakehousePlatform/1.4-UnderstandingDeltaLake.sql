-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

--USE CATALOG hive_metastore

-- COMMAND ----------

drop table employees;

-- COMMAND ----------

CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE)
  USING DELTA
LOCATION 's3://databricks-miraj/workspace/employees/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC Check the created **employees** table in the **Catalog** explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

-- NOTE: With latest Databricks Runtimes, inserting few records in single transaction is optimized into single data file.
-- For this demo, we will insert the records in multiple transactions in order to create 4 data files.

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);


-- COMMAND ----------

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3);

-- NOTE: When executing multiple SQL statements in the same cell, only the last statement's result will be displayed in the cell output.

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Directory

-- COMMAND ----------

DESCRIBE EXTENDED employees;

-- COMMAND ----------

DESCRIBE DETAIL workspace.default.employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "%Kim%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000005.json'

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## When you run update command 
-- MAGIC ###Check the updated file
-- MAGIC - Remove the old file (no DV).
-- MAGIC - Re-add the old file, but with DV applied (marking Sarah’s old row as deleted).
-- MAGIC - Add a tiny new file containing Sarah’s updated row.
-- MAGIC
-- MAGIC ### Even though your SQL only updated 1 row, the Delta log shows:
-- MAGIC
-- MAGIC - 1 remove (the old file version).
-- MAGIC - 1 add (same file, but with a deletion vector).
-- MAGIC - 1 add (the new row file).
-- MAGIC
-- MAGIC That’s why you see “2 files removed” — but really, it’s Delta’s internal bookkeeping for immutability + DVs.un 

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "Sarah";

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Whole flow
-- MAGIC ### 1. We ran an update command
-- MAGIC <br>
-- MAGIC
-- MAGIC ```UPDATE ... WHERE name = 'Sarah'
-- MAGIC operationMetrics: {"numDeletionVectorsAdded":"1","numAddedFiles":"1","numUpdatedRows":"1"}
-- MAGIC ```
-- MAGIC
-- MAGIC - This wrote a new file for Sarah’s updated row.
-- MAGIC - Added a deletion vector on the original file.
-- MAGIC - Net effect: logically 1 row updated.
-- MAGIC
-- MAGIC ### 2. Immediately after → OPTIMIZE shows up
-- MAGIC <br>
-- MAGIC
-- MAGIC ```
-- MAGIC OPTIMIZE {"auto":"true","clusterBy":"[]","zOrderBy":"[]"}
-- MAGIC operationMetrics: {"numRemovedFiles":"2","numAddedFiles":"1","numDeletionVectorsRemoved":"1"}
-- MAGIC ```
-- MAGIC
-- MAGIC Databricks automatically triggered **OPTIMIZE** because our table has auto-optimize / auto-compaction enabled (you can see this in table properties: "delta.enableDeletionVectors":"true" and sometimes "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite"="true").
-- MAGIC
-- MAGIC **What OPTIMIZE did:**
-- MAGIC
-- MAGIC - Took small files (including the DV-marked one) and rewrote them into a single compact file.
-- MAGIC
-- MAGIC - Removed 2 files (the tiny update file + the old DV’d file).
-- MAGIC
-- MAGIC - Wrote back 1 clean file with all rows merged (so no DV left → "numDeletionVectorsRemoved":"1").
-- MAGIC
-- MAGIC **Why this happens**
-- MAGIC
-- MAGIC - Without OPTIMIZE, our Delta Lake would accumulate tiny Parquet files + DVs every time you update small chunks.
-- MAGIC
-- MAGIC - With auto OPTIMIZE, Databricks rewrites them in the background to keep performance healthy.
-- MAGIC
-- MAGIC That’s why after your update, you see an extra OPTIMIZE job cleaning up.
