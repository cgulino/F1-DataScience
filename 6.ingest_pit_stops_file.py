# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using thhe Spark Dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", StringType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
        .option("multiLine", True) \
            .json("/mnt/formula1dlcvtg/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Setp 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceId", "race_id") \
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlcvtg/processed/pit_stops")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlcvtg/processed/pit_stops"))

# COMMAND ----------

