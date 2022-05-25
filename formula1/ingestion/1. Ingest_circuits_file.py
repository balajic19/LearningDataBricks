# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

print(v_data_source)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using Spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllearning/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", StringType(), True),
                                     StructField("lng", StringType(), True),
                                     StructField("alt", StringType(), True),
                                     StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

circuits_df = spark.read.schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

# Style 1
# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
# display(circuits_selected_df)

# COMMAND ----------

# Style 2
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
# display(circuits_selected_df)

# COMMAND ----------

# Style 3
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"].alias("race_location"), circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])
# display(circuits_selected_df)

# COMMAND ----------

# style 4
from pyspark.sql.functions import col, lit
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step- 4 Add ingestion date to the dataframe

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp, lit
# circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
circuits_final_df = add_ingestion_date(circuits_renamed_df)
# .withColumn("env", lit("Production"))
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dllearning/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1dllearning/processed/circuits")
display(df)
# display(spark.read.parquet("/mnt/formula1dllearning/processed/circuits"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")
