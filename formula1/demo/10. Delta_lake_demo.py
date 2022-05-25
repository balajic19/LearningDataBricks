# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 1. Write data to delta lake (Managed table)
# MAGIC 2. Write data to delta lake (External table)
# MAGIC 3. Read data from delta lake (table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Creating f1_demo Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1dllearning/demo"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Reading results df from 23th of March

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json('/mnt/formula1dllearning/raw/2021-03-28/results.json')
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Writing the df in to DELTA MANAGED table or EXTERNAL path
# MAGIC 1. .saveAsTable() creates Managed Table
# MAGIC 2. .save() saves the data file at a particular location for the external table to pickup

# COMMAND ----------

# results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dllearning/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Creating EXTERNAL TABLE at file path we saved earlier

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dllearning/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Describing EXTERNAL TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC TABLE f1_demo.results_external;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### READING delta file from a specific path

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1dllearning/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Writing the df in to DELTA MANAGED table with PARTITION

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Showing PARTITIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### UPDATING the points column for positions less than or equal to 10 in the managed table USING SQL
# MAGIC 
# MAGIC * This creates another copy of the delta file in the managed table location

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### UPDATING the points column for positions less than or equal to 10 in the managed table using PYTHON
# MAGIC 
# MAGIC * This creates another copy of the delta file in the managed table location

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dllearning/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10 ",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### DELETING the data whose position is greater than 10 in the managed table USING SQL
# MAGIC 
# MAGIC * This creates another copy of the delta file in the managed table location

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### DELETING the data whose points is 0 in the managed table USING PYTHON
# MAGIC 
# MAGIC * This creates another copy of the delta file in the managed table location

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dllearning/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### UPDATING the points column for positions less than or equal to 10 in the EXTERNAL table USING PYTHON
# MAGIC 
# MAGIC * This creates another copy of the delta file in the managed table location

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dllearning/demo/results_external')

Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10 ",
  set = { "points": "11 - position" }
)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### DELETING the data whose points is 0 in the EXTERNAL table USING PYTHON
# MAGIC 
# MAGIC * This creates another copy of the delta file in the managed table location

# COMMAND ----------

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Upsert using Merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json('/mnt/formula1dllearning/raw/2021-03-28/drivers.json') \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json('/mnt/formula1dllearning/raw/2021-03-28/drivers.json') \
.filter("driverId BETWEEN 6 and 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json('/mnt/formula1dllearning/raw/2021-03-28/drivers.json') \
.filter("driverId BETWEEN 1 and 5 or driverId BETWEEN 16 and 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Creating a table to work on the UPSERT logic using MERGE statement

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_demo.drivers_merge;
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Using MERGE statement with SQL using day1 view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (upd.driverId ,upd.dob, upd.forename, upd.surname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Using MERGE statement with SQL again but with day2 view

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (upd.driverId ,upd.dob, upd.forename, upd.surname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Using MERGE statement with PYTHON with day3 df

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dllearning/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
.whenMatchedUpdate(set = 
                   {
                       "dob": "upd.dob",
                       "forename": "upd.forename",
                       "surname": "upd.surname",
                       "updatedDate": "current_timestamp()"
                   }) \
.whenNotMatchedInsert(values = {
    "driverId": "upd.driverId",
    "dob": "upd.dob",
    "forename": "upd.forename",
    "surname": "upd.surname",
    "createdDate": "current_timestamp()"
}).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### SQL query to check HISTORY of a TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### SQL query to retrieve specific VERSIONed data of a TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### SQL query to retrieve data updated or created at a specific TIMESTAMP of a TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2022-05-24T01:24:02.000+0000';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### PYTHON command to retrieve data updated or created at a specific TIMESTAMP of a TABLE

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2022-05-24T01:24:02.000+0000").load("/mnt/formula1dllearning/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### VACUUM Command used to delete history older than 7 days
# MAGIC 
# MAGIC But we can change that as well

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2022-05-24T21:14:07.000+0000';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Using the Below command deletes all the history data except for the most recent one (Which is not actually history since its the current data). We can also see the VACUUM command details in the HISTORY command

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### We are getting this error because it's the 2nd updated version of the table and it's been deleted using VACUUM

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2022-05-24T01:10:29.000+0000';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC     ON tgt.driverId = src.driverId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Transaction Logs
# MAGIC 
# MAGIC * We can find the log files (json) in the _delta_log folder 
# MAGIC * If there are multiple jsons (logs), delta creates a checkpoint parquet file (log) after every 9th log file (the log count starts with 0). There will be 3 files at 10th position, 10.json, 10.crc and 10.parquet
# MAGIC * Transaction Logs are kept for 30 DAYS and are removed after 30 days. We can keep them for longer but it is not recommended because every change that we do, a NEW PARQUET FILE IS CREATED IN THE TABLE LOCATION WHICH WILL INCREASE THE COST ON STORAGE and also the PERFORMANCE GETS SLOWER. We can set it to 0 too but it works same as a spark table that doesn't contain time travel, history and stuff

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_demo.drivers_txn;
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn 
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn 
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

for driver_id in range(3, 20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                  SELECT * FROM f1_demo.drivers_merge
                  WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn 
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_demo.drivers_convert_to_delta;
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE)
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### CONVERTING TO DELTA is easy but the transactional log file FIRST CREATES A CHECKPOINT followed by the log file (json)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### CONVERTING A FILE TO DELTA is slightly different as follows

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dllearning/demo/drivers_convert_to_delta_new")
# df.write.parquet("/mnt/formula1dllearning/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dllearning/demo/drivers_convert_to_delta_new`

# COMMAND ----------


