-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df = spark.read.json(f"{dataset_bookstore}/customers-json/export_001.json")
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

SELECT * FROM json.`s3://databricks-miraj/bookstore/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`s3://databricks-miraj/bookstore/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`s3://databricks-miraj/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The _metadata Column
-- MAGIC The input_file_name() function is no longer supported in newer versions of the Databricks Runtime. As an alternative, you can use the _metadata.file_path attribute to retrieve the file path information.
-- MAGIC <br><br>
-- MAGIC
-- MAGIC ```
-- MAGIC SELECT *,
-- MAGIC        _metadata.file_path AS source_file
-- MAGIC FROM json.`${dataset.bookstore}/customers-json`;
-- MAGIC ```
-- MAGIC
-- MAGIC By leveraging the _metadata column, you can access various details about your input files, such as:
-- MAGIC
-- MAGIC **_metadata.file_path:** The full path to the input file.
-- MAGIC
-- MAGIC **_metadata.file_name:** The name of the file, including its extension.
-- MAGIC
-- MAGIC **_metadata.file_size:** The size of the file in bytes.
-- MAGIC
-- MAGIC **_metadata.file_modification_time:** The timestamp of the last modification made to the file.
-- MAGIC
-- MAGIC

-- COMMAND ----------

 SELECT *,
    _metadata.file_path AS source_file
  FROM json.`s3://databricks-miraj/bookstore/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`s3://databricks-miraj/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`s3://databricks-miraj/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "s3://databricks-miraj/bookstore/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simplified File Querying
-- MAGIC Databricks recently introduced a new function called read_files that makes it easier to query CSV files and other file formats directly, without needing to first create a temporary view.
-- MAGIC
-- MAGIC Example: Querying CSV Files
-- MAGIC
-- MAGIC ```
-- MAGIC SELECT * FROM read_files(
-- MAGIC   '${dataset_bookstore}/books-csv/export_*.csv',
-- MAGIC   format => 'csv',
-- MAGIC   header => 'true',
-- MAGIC   delimiter => ';'
-- MAGIC );
-- MAGIC
-- MAGIC ````
-- MAGIC
-- MAGIC Now, we can create our books delta table directly from these files using a CTAS statement:
-- MAGIC
-- MAGIC ```
-- MAGIC CREATE TABLE books
-- MAGIC AS SELECT * FROM read_files(
-- MAGIC     '${dataset_bookstore}/books-csv/export_*.csv',
-- MAGIC     format => 'csv',
-- MAGIC     header => 'true',
-- MAGIC     delimiter => ';'
-- MAGIC );
-- MAGIC ```
-- MAGIC The read_files function automatically tries to infer a unified schema from all the source files. If any value doesn’t match the expected schema, it's stored in an extra column called _rescued_data as a JSON string.

-- COMMAND ----------

SELECT * FROM read_files(
  's3://databricks-miraj/bookstore/books-csv/export_*.csv',
  format => 'csv',
  header => 'true',
  delimiter => ';'
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`s3://databricks-miraj/bookstore/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`s3://databricks-miraj/bookstore//books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books
