# Databricks notebook source

from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from typing import Iterator
from pyspark.sql import Row

class NativeDistributedProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.total_sum = handle.getValueState("totalSum", "count INT")

    def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
        user_id = key[0]
        batch_sum = 0
    
        for row in rows:
            if row.amount is not None:
                batch_sum += int(row.amount) 

        existing = self.total_sum.get()
        old_total = existing[0] if existing else 0
        new_total = old_total + batch_sum
        self.total_sum.update((new_total,))

        yield Row(id=user_id, countAsString=str(new_total))

# Command ----------

batch_1_df = spark.readStream.table("stateful_processor.default.streaming_query")

output_schema = "id STRING, countAsString STRING"

streaming_query = (
    batch_1_df.groupBy("user_id")
    .transformWithState(
        statefulProcessor=NativeDistributedProcessor(),
        outputStructType=output_schema,
        outputMode="Update",
        timeMode="None"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/stateful_processor/default/silver/CheckPoint/")
    .trigger(availableNow=True)
    .toTable("stateful_processor.default.streaming_query_silver")
)