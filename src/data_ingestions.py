# Databricks notebook source

from pyspark.sql.functions import col, to_timestamp

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/stateful_processor/default/raw_data/SchemaLocation/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/stateful_processor/default/raw_data/*.csv")
    .withColumn("amount", col("amount").cast("int"))
    .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"))
)


# COMMAND ----------
df.writeStream.format("delta").option("checkpointLocation", "/Volumes/stateful_processor/default/raw_data/CheckPoint/").outputMode("append").trigger(availableNow=True).toTable("stateful_processor.default.streaming_query")