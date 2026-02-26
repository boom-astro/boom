"""Script to read the Kafka output from the BOOM throughput test."""
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "confluent-kafka",
# ]
# ///

from confluent_kafka import Consumer, KafkaException

# Now let's check that we can read all the alerts from
# Kafka topic
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "throughput-benchmarking-verify",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
topic_pattern = "^babamul\\.ztf\\..*"
consumer.subscribe([topic_pattern])
n_alerts = 0
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            break
        if msg.error():
            raise KafkaException(msg.error())
        n_alerts += 1
finally:
    consumer.close()

if n_alerts != 28548:
    raise AssertionError(f"Expected 28548 alerts, but got {n_alerts}")

print(f"Read {n_alerts} alerts from all Kafka topics matching {topic_pattern}")
