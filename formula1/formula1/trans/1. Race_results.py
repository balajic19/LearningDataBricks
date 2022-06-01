# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("date", "race_date").withColumnRenamed("time", "race_time")
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name").withColumnRenamed("location", "circuit_location")
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name", "constructor_name")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

display(races_df)
display(circuits_df)
display(drivers_df)
display(constructors_df)
display(results_df)

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date
final_df = race_results_df.select(race_results_df["result_race_id"], race_results_df["race_year"], race_results_df["race_name"], race_results_df["race_date"],
                                  race_results_df["circuit_location"], race_results_df["driver_name"], race_results_df["driver_number"],\
                                  race_results_df["driver_nationality"], race_results_df["constructor_name"].alias("team"), race_results_df["grid"],\
                                  race_results_df["fastest_lap"], race_results_df["time"].alias("race_time"), race_results_df["points"],\
                                  race_results_df["position"], race_results_df["result_file_date"])\
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date
join_results_race_df = results_df.join(races_df, results_df.result_race_id == races_df.race_id)
join_df = join_results_race_df.join(circuits_df, join_results_race_df.circuit_id == circuits_df.circuit_id) \
                    .join(drivers_df, join_results_race_df.driver_id == drivers_df.driver_id) \
                    .join(constructors_df, join_results_race_df.constructor_id == constructors_df.constructor_id) \
                    .select(join_results_race_df.result_race_id, join_results_race_df.race_year, join_results_race_df.race_name, join_results_race_df.race_date, circuits_df.circuit_location, drivers_df.driver_name, drivers_df.driver_number,
                           drivers_df.driver_nationality, constructors_df.constructor_name.alias("team"), join_results_race_df.grid, join_results_race_df.fastest_lap, join_results_race_df.time.alias("race_time"), join_results_race_df.points, join_results_race_df.position, join_results_race_df.result_file_date) \
                    .withColumn("created_date", current_timestamp()) \
                    .withColumnRenamed("result_file_date", "file_date")


# COMMAND ----------

# display(join_df)
# display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Just checking if my join_df is similar to course's final_df

# COMMAND ----------

display(join_df.describe())
display(final_df.describe())

# COMMAND ----------

# display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy("points", ascending=False))

# COMMAND ----------

# display(join_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy("points", ascending=False))

# COMMAND ----------

# join_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# overwrite_partition(join_df, 'f1_presentation', 'race_results', 'result_race_id')
# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')
# join_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.result_race_id = src.result_race_id"
merge_delta_data(join_df, "f1_presentation", "race_results", presentation_folder_path, merge_condition, 'result_race_id')

# COMMAND ----------

# display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllearning/presentation/race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT result_race_id, COUNT(1) as count
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY result_race_id
# MAGIC ORDER BY result_race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")