# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name").withColumnRenamed("date", "race_date").withColumnRenamed("time", "race_time")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name").withColumnRenamed("location", "circuit_location")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "constructor_name")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

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

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date
final_df = race_results_df.select(race_results_df["race_year"], race_results_df["race_name"], race_results_df["race_date"],
                                  race_results_df["circuit_location"], race_results_df["driver_name"], race_results_df["driver_number"],\
                                  race_results_df["driver_nationality"], race_results_df["constructor_name"].alias("team"), race_results_df["grid"],\
                                  race_results_df["fastest_lap"], race_results_df["time"].alias("race_time"), race_results_df["points"],\
                                  race_results_df["position"])\
                          .withColumn("created_date", current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date
join_results_race_df = results_df.join(races_df, results_df.race_id == races_df.race_id)
join_df = join_results_race_df.join(circuits_df, join_results_race_df.circuit_id == circuits_df.circuit_id) \
                    .join(drivers_df, join_results_race_df.driver_id == drivers_df.driver_id) \
                    .join(constructors_df, join_results_race_df.constructor_id == constructors_df.constructor_id) \
                    .select(join_results_race_df.race_year, join_results_race_df.race_name, join_results_race_df.race_date, circuits_df.circuit_location, drivers_df.driver_name, drivers_df.driver_number,
                           drivers_df.driver_nationality, constructors_df.constructor_name.alias("team"), join_results_race_df.grid, join_results_race_df.fastest_lap, join_results_race_df.time.alias("race_time"), join_results_race_df.points, join_results_race_df.position) \
                    .withColumn("created_date", current_timestamp())


# COMMAND ----------

display(join_df)
display(final_df)

# COMMAND ----------

join_df.printSchema()
final_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Just checking if my join_df is similar to course's final_df

# COMMAND ----------

display(join_df.describe())
display(final_df.describe())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy("points", ascending=False))

# COMMAND ----------

display(join_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy("points", ascending=False))

# COMMAND ----------

# join_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

join_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllearning/presentation/race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;
