-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⚙️ Higher-Order Functions in Databricks SQL
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🧠 Definition
-- MAGIC Higher-order functions are **functions that operate on arrays or maps** and can **take other functions as arguments** or **return functions**.  
-- MAGIC They allow you to perform transformations, filters, or aggregations on complex data types (like arrays) directly within SQL — **without exploding them.**
-- MAGIC
-- MAGIC In simple terms:
-- MAGIC > A higher-order function lets you apply logic *inside* an array or map column, element by element.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 💡 Common Higher-Order Functions
-- MAGIC
-- MAGIC | Function | Description | Example |
-- MAGIC |-----------|--------------|----------|
-- MAGIC | `transform()` | Applies a function to each element in an array | `transform(array(1,2,3), x -> x + 1)` → `[2,3,4]` |
-- MAGIC | `filter()` | Keeps elements that satisfy a condition | `filter(array(1,2,3,4), x -> x % 2 = 0)` → `[2,4]` |
-- MAGIC | `exists()` | Returns `true` if any element matches condition | `exists(array(1,2,3), x -> x = 2)` → `true` |
-- MAGIC | `reduce()` | Aggregates array elements using an expression | `reduce(array(1,2,3), 0, (acc, x) -> acc + x)` → `6` |
-- MAGIC | `aggregate()` | More general version of reduce with final transformation | `aggregate(array(1,2,3), 0, (acc, x) -> acc + x, acc -> acc * 10)` → `60` |
-- MAGIC | `transform_keys()` | Transforms keys in a map | `transform_keys(map('a',1,'b',2), (k,v) -> upper(k))` → `{"A":1,"B":2}` |
-- MAGIC | `transform_values()` | Transforms values in a map | `transform_values(map('a',1,'b',2), (k,v) -> v*2)` → `{"a":2,"b":4}` |
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ## 🧩 Example
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Create sample data
-- MAGIC SELECT array(10, 20, 30) AS numbers;
-- MAGIC
-- MAGIC -- Apply higher-order function
-- MAGIC SELECT transform(numbers, x -> x / 10) AS divided
-- MAGIC FROM (SELECT array(10, 20, 30) AS numbers);
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filtering Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_copies
FROM orders

-- COMMAND ----------

SELECT order_id, multiple_copies
FROM (
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Transforming Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## What is a UDF in Databricks?
-- MAGIC
-- MAGIC A **User-Defined Function (UDF)** in Databricks is a custom function created by users to extend the built-in functionality of Apache Spark SQL or the Spark API. UDFs allow you to define specific logic that may not be readily supported with the native Spark functions.
-- MAGIC
-- MAGIC ### Key Points about UDFs:
-- MAGIC
-- MAGIC - **Purpose:**
-- MAGIC Perform complex calculations, transformations, or data manipulations that are difficult or impossible with built-in Spark functions.
-- MAGIC - **Languages Supported:**
-- MAGIC You can write UDFs in **Python, Scala, Java, and R**.
-- MAGIC - **Use Cases:**
-- MAGIC     - Data encryption/decryption
-- MAGIC     - Hashing
-- MAGIC     - JSON parsing
-- MAGIC     - Custom validation logic
-- MAGIC     - Business-specific calculations
-- MAGIC - **Performance Note:**
-- MAGIC Built-in Spark functions are usually faster and optimized for distributed processing. Use UDFs primarily when no built-in function meets your requirement.
-- MAGIC - **Scope:**
-- MAGIC UDFs can be temporary (session-scoped) or registered persistently for reuse.
-- MAGIC
-- MAGIC
-- MAGIC ### Example: Defining a Python UDF in Databricks
-- MAGIC
-- MAGIC ```python
-- MAGIC from pyspark.sql.functions import udf
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC
-- MAGIC # Define a python function
-- MAGIC def capitalize_first_letter(s):
-- MAGIC     if s:
-- MAGIC         return s.capitalize()
-- MAGIC     else:
-- MAGIC         return None
-- MAGIC
-- MAGIC # Register the function as a Spark UDF
-- MAGIC capitalize_udf = udf(capitalize_first_letter, StringType())
-- MAGIC
-- MAGIC # Use in DataFrame operations
-- MAGIC df = spark.createDataFrame([("alice",), ("bob",), (None,)], ["name"])
-- MAGIC df.withColumn("capitalized_name", capitalize_udf("name")).show()
-- MAGIC ```
-- MAGIC
-- MAGIC
-- MAGIC ### SQL UDFs in Databricks SQL
-- MAGIC
-- MAGIC Databricks also supports **SQL UDFs**, which are functions written purely in SQL for reuse and better integration with SQL workflows. They are easier for SQL users and optimize well with the query optimizer.
-- MAGIC
-- MAGIC ***
-- MAGIC
-- MAGIC ### References
-- MAGIC
-- MAGIC - [Databricks What are User-Defined Functions (UDFs)?](https://docs.databricks.com/aws/en/udf/)
-- MAGIC - [Databricks SQL User-Defined Functions](https://www.databricks.com/blog/2021/10/20/introducing-sql-user-defined-functions.html)
-- MAGIC - [Azure Databricks UDF Documentation](https://learn.microsoft.com/en-us/azure/databricks/udf/)
-- MAGIC
-- MAGIC UDFs offer great flexibility for custom processing in Spark workloads when built-in functions don’t suffice.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;
