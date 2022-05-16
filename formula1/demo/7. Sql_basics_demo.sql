-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM drivers;

-- COMMAND ----------

DESC EXTENDED drivers;

-- COMMAND ----------

SELECT name, dob
  FROM drivers
WHERE nationality = 'British'
  AND dob >= '1990-01-01'
ORDER BY dob DESC;

-- COMMAND ----------

SELECT name, nationality, dob
  FROM drivers
WHERE (nationality = 'British'
  AND dob >= '1990-01-01')
  OR
  nationality = 'Indian'
ORDER BY dob DESC;

-- COMMAND ----------


