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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# header=["raceId", "driverId", "lap", "position", "time", ]
lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")
display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

display(lap_times_final_df)

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) as count
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) as count
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")