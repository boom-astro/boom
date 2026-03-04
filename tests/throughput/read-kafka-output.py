"""Script to read the Kafka output from the BOOM throughput test."""
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "confluent-kafka",
# ]
# ///

from confluent_kafka import Consumer, KafkaException

# Now let's check that we can read all of the alerts from the babamul.ztf.none
# Kafka topic
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "throughput-benchmarking-verify",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
topic = "^babamul*"
consumer.subscribe([topic])
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

n_expected = 22674

if n_alerts != n_expected:
    raise RuntimeError(f"Expected {n_expected} alerts, but got {n_alerts}")

print(f"Read {n_alerts} alerts from topic {topic}")
