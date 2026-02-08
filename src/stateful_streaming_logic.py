# Databricks notebook source

from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql import Row
from typing import Iterator
from datetime import timedelta

class SessionFraudProcessor(StatefulProcessor):

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.txn_state = handle.getValueState(
            "txnState",
            "timestamps ARRAY<TIMESTAMP>, amounts ARRAY<INT>, sessionTotal INT"
        )

    def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
        user_id = key[0]

        existing = self.txn_state.get()
        if existing:
            timestamps = list(existing[0])
            amounts = list(existing[1])
            session_total = existing[2]
        else:
            timestamps = []
            amounts = []
            session_total = 0

        latest_event_time = None

        for row in rows:
            if row.event_time and row.amount is not None:
                timestamps.append(row.event_time)
                amounts.append(int(row.amount))
                session_total += int(row.amount)
                latest_event_time = row.event_time

        if latest_event_time:
            # keep only last 60 sec window
            window_start = latest_event_time - timedelta(seconds=60)

            filtered = [
                (ts, amt) for ts, amt in zip(timestamps, amounts)
                if ts >= window_start
            ]

            timestamps = [x[0] for x in filtered]
            amounts = [x[1] for x in filtered]

            # set inactivity timer (2 min)
            timerValues.setTimer(latest_event_time + timedelta(minutes=2))

        self.txn_state.update((timestamps, amounts, session_total))

        # burst + spend rule
        suspicious = len(timestamps) > 5 and sum(amounts) > 500

        yield Row(
            user_id=user_id,
            txn_last_60s=len(timestamps),
            spend_last_60s=sum(amounts),
            session_total=session_total,
            suspicious=suspicious,
            event_type="update"
        )

    def handleExpiredTimer(self, key, timerValues):
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

        # clear state after expiry
        self.txn_state.clear()

# COMMAND ----------
batch_df = spark.readStream.table("stateful_processor.default.streaming_query")

output_schema = """
user_id STRING,
txn_last_60s INT,
spend_last_60s INT,
session_total INT,
suspicious BOOLEAN,
event_type STRING
"""

streaming_query = (
    batch_df
    .groupBy("user_id")
    .transformWithState(
        statefulProcessor=SessionFraudProcessor(),
        outputStructType=output_schema,
        outputMode="Update",
        timeMode="EventTime"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(available_now = True)
    .option("checkpointLocation", "/Volumes/stateful_processor/default/silver/CheckPoint/")
    .toTable("stateful_processor.default.streaming_query_silver")
)

