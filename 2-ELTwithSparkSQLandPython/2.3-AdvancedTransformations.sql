-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Parsing JSON Data

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Json string
-- MAGIC Can you see the **profile** column in customers table is not a string , its **json** string.

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country 
FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This will fail as **from_json** requires schema of our object

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
  FROM customers;

-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 🧱 What Is STRUCT in Databricks SQL
-- MAGIC
-- MAGIC **STRUCT** is a complex (nested) data type — it’s like a row within a row.
-- MAGIC Think of it as a JSON object or a small record stored inside a column.
-- MAGIC
-- MAGIC _In SQL terms:_
-- MAGIC
-- MAGIC A **STRUCT** groups multiple fields together into a single column.

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **profile_struct.** will extract all the columns

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode Function

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here books column is of Array type:
-- MAGIC ```
-- MAGIC array
-- MAGIC 0: {"book_id": "B07", "quantity": 1, "subtotal": 33}
-- MAGIC 1: {"book_id": "B06", "quantity": 1, "subtotal": 22}
-- MAGIC ```

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book 
FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collecting Rows

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **collect_set** will collect unique values

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Flatten Arrays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This Spark SQL query performs aggregation and array transformations per customer. Here's what it does step-by-step:
-- MAGIC <br><br>
-- MAGIC ```sql
-- MAGIC SELECT customer_id,
-- MAGIC   collect_set(books.book_id) As before_flatten,
-- MAGIC   array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
-- MAGIC FROM orders
-- MAGIC GROUP BY customer_id
-- MAGIC ```
-- MAGIC
-- MAGIC
-- MAGIC ### Breaking it Down
-- MAGIC
-- MAGIC 1. `collect_set(books.book_id) AS before_flatten`
-- MAGIC     - For each `customer_id`, this collects unique arrays of `book_id`s from the `orders` table.
-- MAGIC     - If `books.book_id` is itself an array type, `collect_set` collects a **set of arrays** (an array of arrays), keeping only unique entries.
-- MAGIC 2. `flatten(collect_set(books.book_id))`
-- MAGIC     - Since `collect_set(books.book_id)` returns an array of arrays, `flatten()` merges these nested arrays into a single one-level array.
-- MAGIC 3. `array_distinct(...) AS after_flatten`
-- MAGIC     - After flattening, duplicate `book_id`s may appear in the merged array.
-- MAGIC     - `array_distinct` removes duplicate values, leaving only unique book IDs per customer.
-- MAGIC
-- MAGIC ### Summary of Results
-- MAGIC
-- MAGIC - **before_flatten:** an array of unique arrays per customer (array of arrays, no repeated nested arrays)
-- MAGIC - **after_flatten:** a single flat array of unique book IDs per customer with duplicates removed
-- MAGIC
-- MAGIC
-- MAGIC ### Example:
-- MAGIC
-- MAGIC Assuming a customer has ordered books with IDs like: `[ [B01, B02], [B02, B03], [B01] ]`
-- MAGIC
-- MAGIC - `collect_set` produces something like: `[ [B01, B02], [B02, B03], [B01] ]` (only unique arrays)
-- MAGIC - `flatten` merges into: `[ B01, B02, B02, B03, B01 ]`
-- MAGIC - `array_distinct` reduces to: `[ B01, B02, B03 ]`
-- MAGIC
-- MAGIC
-- MAGIC ### Function References:
-- MAGIC
-- MAGIC - [`collect_set(expr)`](https://spark.apache.org/docs/latest/api/sql/index.html#collect_set) : Returns an array of unique elements.
-- MAGIC - [`flatten(array)`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#flatten) : Flattens an array of arrays into a single array.
-- MAGIC - [`array_distinct(array)`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/array_distinct.html) : Removes duplicate values from an array.
-- MAGIC
-- MAGIC ***
-- MAGIC
-- MAGIC This pattern is useful to aggregate nested arrays across rows, combine them into a single flattened list, and ensure uniqueness — often used in scenarios involving multi-valued attributes aggregated per key.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Join Operations

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Operations

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 

-- COMMAND ----------

SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 

-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshaping Data with Pivot

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This SQL query uses the **PIVOT** operator to transform the `orders_enriched` data from a row-based format into a wide, column-based format where each book ID gets its own column.
-- MAGIC
-- MAGIC ### **Query Structure**
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE OR REPLACE TABLE transactions AS
-- MAGIC SELECT * FROM (
-- MAGIC   SELECT
-- MAGIC     customer_id,
-- MAGIC     book.book_id AS book_id,
-- MAGIC     book.quantity AS quantity
-- MAGIC   FROM orders_enriched
-- MAGIC ) PIVOT (
-- MAGIC   sum(quantity) FOR book_id in (
-- MAGIC     'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
-- MAGIC     'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
-- MAGIC   )
-- MAGIC );
-- MAGIC ```
-- MAGIC
-- MAGIC
-- MAGIC ### **Step-by-Step Explanation**
-- MAGIC
-- MAGIC 1. **Inner Query** (`SELECT customer_id, book.book_id AS book_id, book.quantity AS quantity FROM orders_enriched`):
-- MAGIC     - Extracts each customer's ID, the book IDs, and the quantity for every order—from the possibly nested or expanded `orders_enriched` data.
-- MAGIC     - Resulting table (before pivot):
-- MAGIC | customer_id | book_id | quantity |
-- MAGIC | :-- | :-- | :-- |
-- MAGIC | 101 | B01 | 2 |
-- MAGIC | 101 | B02 | 1 |
-- MAGIC | 102 | B01 | 1 |
-- MAGIC | 102 | B03 | 3 |
-- MAGIC
-- MAGIC 2. **Pivot Operation**:
-- MAGIC     - Pivots `book_id`, creating new columns for each listed book.
-- MAGIC     - For each `customer_id`, sums the `quantity` for each book in the provided list (`B01`, … `B12`).
-- MAGIC     - Final table layout:
-- MAGIC | customer_id | B01 | B02 | B03 | ... | B12 |
-- MAGIC | :-- | :-- | :-- | :-- | :-- | :-- |
-- MAGIC | 101 | 2 | 1 | 0 | ... | 0 |
-- MAGIC | 102 | 1 | 0 | 3 | ... | 0 |
-- MAGIC
-- MAGIC - Missing quantities appear as `0` or `NULL` (based on engine—most default to 0 with `sum()`).
-- MAGIC
-- MAGIC 3. **Why Use PIVOT?**
-- MAGIC     - The pivot operation is handy for generating one row per customer, with a column for each book they bought, storing the total quantity.
-- MAGIC     - Makes it easier to analyze purchase patterns, visualize inventories, or run further aggregations.
-- MAGIC
-- MAGIC ### **What Happens Under the Hood?**
-- MAGIC
-- MAGIC - The aggregation function (`sum(quantity)`) computes the total for each book per customer.
-- MAGIC - The `FOR book_id IN (...)` clause specifies which book IDs should be converted into columns.
-- MAGIC
-- MAGIC
-- MAGIC ### **Visual Example (Before and After Pivot)**
-- MAGIC
-- MAGIC **Input Table:**
-- MAGIC
-- MAGIC
-- MAGIC | customer_id | book_id | quantity |
-- MAGIC | :-- | :-- | :-- |
-- MAGIC | 101 | B01 | 2 |
-- MAGIC | 101 | B02 | 1 |
-- MAGIC | 102 | B01 | 1 |
-- MAGIC | 102 | B03 | 3 |
-- MAGIC | ... | ... | ... |
-- MAGIC
-- MAGIC **Pivoted Table:**
-- MAGIC
-- MAGIC
-- MAGIC | customer_id | B01 | B02 | B03 | ... | B12 |
-- MAGIC | :-- | :-- | :-- | :-- | :-- | :-- |
-- MAGIC | 101 | 2 | 1 | 0 | ... | 0 |
-- MAGIC | 102 | 1 | 0 | 3 | ... | 0 |
-- MAGIC | ... | ... | ... | ... | ... | ... |
-- MAGIC
-- MAGIC ### **Summary**
-- MAGIC
-- MAGIC - **CREATE OR REPLACE TABLE** statement creates a new table (`transactions`) with the wide format.
-- MAGIC - **PIVOT** operation is useful for understanding customer buying patterns and generating summary reports quickly.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
