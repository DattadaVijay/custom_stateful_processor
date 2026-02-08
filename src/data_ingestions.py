# Databricks notebook source

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "/Volumes/stateful_processor/default/raw_data/SchemaLocation/")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("/Volumes/stateful_processor/default/raw_data/*.csv"))

# Command ----------
df.writeStream.format("delta").option("checkpointLocation", "/Volumes/stateful_processor/default/raw_data/CheckPoint/").outputMode("append").trigger(availableNow=True).toTable("stateful_processor.default.streaming_query")