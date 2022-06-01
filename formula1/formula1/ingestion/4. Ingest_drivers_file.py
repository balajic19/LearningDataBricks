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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)]
                        )

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema, True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                   ])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")
display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

drivers_dropped_df = drivers_df.drop(drivers_df["url"])
drivers_final_df = drivers_dropped_df.withColumnRenamed("driverId", "driver_id") \
                                     .withColumnRenamed("driverRef", "driver_ref") \
                                     .withColumn("ingestion_date", current_timestamp()) \
                                     .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                     .withColumn("data_source", lit(v_data_source)) \
                                     .withColumn("file_date", lit(v_file_date))

display(drivers_final_df)

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/drivers"))
display(spark.sql("""SELECT * FROM f1_processed.drivers"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

