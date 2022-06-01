# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("name", "circuit_name")
races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_df)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Inner Joins

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id) \
                               .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Outer Joins

# COMMAND ----------

# Left Outer join

circuits_df = circuits_df.filter("circuit_id < 70")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
                               .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# Right Outer join
races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
                               .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)


# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# Full Outer Join

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
                               .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Joins

# COMMAND ----------

# Same as inner join but we do not have access to select cols from right df and we also return only the left cols as the output
# Throws error as we are using the cols from right df
# races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
#                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round) 

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")


# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti join

# COMMAND ----------

# Returns everything on the left Df that is not found on the right df

# races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \
#                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round) 
races_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti") 

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Cross Joins

# COMMAND ----------

races_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

races_circuits_df.count()

# COMMAND ----------

