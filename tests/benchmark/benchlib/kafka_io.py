"""Kafka helpers: topic setup and reading BOOM's output back."""

from __future__ import annotations

import io
import re
import time

import fastavro
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

# What BOOM publishes: Babamul's public streams, and each survey's filter results.
OUTPUT_TOPIC_PATTERN = r"^(babamul\..*|[A-Z]+_alerts_results)$"


def create_topics(bootstrap: str, topics: list[str], partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=30).topics)
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1)
           for t in sorted(set(topics)) if t not in existing]
    if not new:
        return
    for topic, future in admin.create_topics(new).items():
        try:
            future.result(timeout=60)
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise RuntimeError(f"could not create topic {topic}: {e}") from e


def output_survey(topic: str) -> str:
    if topic.startswith("babamul."):
        return topic.split(".")[1].upper()
    return topic.split("_")[0]


def read_outputs(bootstrap: str, idle_s: float = 10.0, pattern: str = OUTPUT_TOPIC_PATTERN):
    """Yield (topic, timestamp_ms, record) for every message in BOOM's output topics.

    Reads from the beginning in a fresh group and stops once every partition is
    at its end. The timestamp is the broker's CreateTime, i.e. when BOOM
    produced the message.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    regex = re.compile(pattern)
    topics = [t for t in admin.list_topics(timeout=30).topics if regex.match(t)]
    if not topics:
        return
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"boom-bench-collect-{time.time_ns()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.message.max.bytes": 50 * 1024 * 1024,
    })
    try:
        assignments, ends = [], {}
        meta = consumer.list_topics(timeout=30)
        for t in topics:
            for p in meta.topics[t].partitions:
                tp = TopicPartition(t, p, 0)
                low, high = consumer.get_watermark_offsets(tp, timeout=30)
                if high > low:
                    assignments.append(TopicPartition(t, p, low))
                    ends[(t, p)] = high
        if not assignments:
            return
        consumer.assign(assignments)
        remaining = dict(ends)
        last_msg = time.time()
        while remaining and time.time() - last_msg < idle_s:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            last_msg = time.time()
            key = (msg.topic(), msg.partition())
            if msg.offset() + 1 >= remaining.get(key, 0):
                remaining.pop(key, None)
            _, ts = msg.timestamp()
            try:
                record = next(iter(fastavro.reader(io.BytesIO(msg.value()))))
            except Exception:
                record = None
            yield msg.topic(), ts, record
    finally:
        consumer.close()
