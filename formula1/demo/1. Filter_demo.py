# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_filtered_df = races_df.filter("race_year = 2019 and round <=5 ")
race_filtered_df.show()

# COMMAND ----------

display(races_df.filter(races_df["race_year"] == 2019))

# COMMAND ----------

display(races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5)))

# COMMAND ----------

