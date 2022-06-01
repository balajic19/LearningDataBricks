# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", IntegerType(), True)
                                   ])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")
display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
                             .withColumnRenamed("raceId", "race_id") \
                             .withColumnRenamed("driverId", "driver_id") \
                             .withColumnRenamed("constructorId", "constructor_id") \
                             .withColumnRenamed("positionText", "position_text") \
                             .withColumnRenamed("positionOrder", "position_order") \
                             .withColumnRenamed("fastestLap", "fastest_lap") \
                             .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                             .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                             .withColumn("ingestion_date", current_timestamp()) \
                             .withColumn("data_source", lit(v_data_source)) \
                             .withColumn("file_date", lit(v_file_date)) \
                             .drop(results_df["statusId"])

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Drop Duplicates

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 1 of Incremental Load

# COMMAND ----------

# for each_race_id in results_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {each_race_id.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").format("parquet").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Method 2 of Incremental Load

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS f1_processed.results

# COMMAND ----------

# def re_arrange_partition_column(input_df, partition_column):
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)
# #     print(column_list)

#     output_df = input_df.select(column_list)
#     return output_df

# results_final_df = re_arrange_partition_column(input_df, partition_column)

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     # Expects the df to have the partition column at the end
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("overwrite").format("parquet").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Method 1 and 2 doesn't work for delta tables becasue delta tables doesnt support partitionOverwriteMode = dynamic in the method. It might work in the future because there's a Jira ticket to solve this issue
# MAGIC 
# MAGIC * Set this command to true, spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true") for dynamic partition pruning that means use the race_id as partition instead of static partitioning (tgt.race_id = src.race_id [dynamuic pruning] and tgt.race_id = 1052 [static pruning])

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
# from delta.tables import DeltaTable

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dllearning/processed/results")
#     deltaTable.alias("tgt").merge(
#     results_final_df.alias("src"),
#     "tgt.result_id = src.result_id AND tgt.race_id = src.race_id") \
#     .whenMatchedUpdateAll() \
#     .whenNotMatchedInsertAll()\
#     .execute()
# else:
#     results_final_df.write.mode("overwrite").format("delta").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, "f1_processed", "results", processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllearning/processed/results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) as count
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1) as count
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESC HISTORY f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

