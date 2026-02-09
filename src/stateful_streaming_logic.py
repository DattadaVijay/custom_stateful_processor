# Databricks notebook source

from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql import Row
from typing import Iterator
from datetime import timedelta
# COMMAND ----------
class SessionFraudProcessor(StatefulProcessor):

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.txn_state = handle.getValueState(
            "txnState",
            "timestamps ARRAY<TIMESTAMP>, amounts ARRAY<INT>, sessionTotal INT"
        )
        self.handle = handle

    def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
        user_id = key[0]

        existing = self.txn_state.get()
        if existing:
            timestamps = list(existing[0])
            amounts = list(existing[1])
            session_total = existing[2]
        else:
            timestamps, amounts, session_total = [], [], 0

        latest_event_time = None

        for row in rows:
            if row.event_time and row.amount is not None:
                timestamps.append(row.event_time)
                amounts.append(int(row.amount))
                session_total += int(row.amount)
                if latest_event_time is None or row.event_time > latest_event_time:
                    latest_event_time = row.event_time

        if latest_event_time:
            window_start = latest_event_time - timedelta(seconds=60)
            filtered = [(ts, amt) for ts, amt in zip(timestamps, amounts) if ts >= window_start]
            
            timestamps = [x[0] for x in filtered]
            amounts = [x[1] for x in filtered]
            timeout_ms = int((latest_event_time + timedelta(minutes=2)).timestamp() * 1000)
            self.handle.registerTimer(timeout_ms)
        self.txn_state.update((timestamps, amounts, session_total))

        # Fraud Rule: > 5 transactions AND > $500 in the 60s sliding window
        suspicious = len(timestamps) > 5 and sum(amounts) > 500

        yield Row(
            user_id=user_id,
            txn_last_60s=len(timestamps),
            spend_last_60s=sum(amounts),
            session_total=session_total,
            suspicious=suspicious,
            event_type="update"
        )

    def handleExpiration(self, key, expirationTimerTimestampInMs) -> Iterator[Row]:
        user_id = key[0]
        existing = self.txn_state.get()
        
        if existing:
            timestamps, amounts, session_total = existing
            yield Row(
                user_id=user_id,
                txn_last_60s=len(timestamps),
                spend_last_60s=sum(amounts),
                session_total=session_total,
                suspicious=False,
                event_type="session_expired"
            )
        self.txn_state.clear()
# COMMAND ----------
batch_df = spark.readStream.table("stateful_processor.default.streaming_query")

output_schema = """
    user_id STRING,
    event_time TIMESTAMP,
    txn_last_60s INT,
    spend_last_60s INT,
    session_total INT,
    suspicious BOOLEAN,
    event_type STRING
"""

# COMMAND ----------
streaming_query = (
    batch_df
    .withWatermark("event_time", "5 minutes")
    .groupBy("user_id")
    .transformWithState(
        statefulProcessor=SessionFraudProcessor(),
        outputStructType=output_schema,
        outputMode="Update",
        timeMode="EventTime",
        eventTimeColumnName="event_time"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(once=True)
    .option("checkpointLocation", "/Volumes/stateful_processor/default/silver/CheckPoint/")
    .toTable("stateful_processor.default.streaming_query_silver")
)