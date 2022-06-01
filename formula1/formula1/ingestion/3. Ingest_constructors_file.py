# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")
display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")
# constructor_dropped_df = constructor_df.drop(constructor_df["url"])
# constructor_dropped_df = constructor_df.drop(col("url"))
display(constructor_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file-date", lit(v_file_date))
display(constructor_final_df)

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllearning/processed/constructors

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/constructors"))
display(spark.sql("""SELECT * FROM f1_processed.constructors"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

