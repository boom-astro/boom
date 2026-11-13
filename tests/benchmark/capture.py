"""Record a live alert stream into a replay bundle.

LSST has no nightly archive BOOM can download, so a benchmark night has to be
captured as it streams. Packets are stored byte for byte together with the
broker timestamp each arrived with, so the night can be replayed on the clock
it was actually delivered on (`replay.py --clock kafka`).

    export CAPTURE_KAFKA_USERNAME=... CAPTURE_KAFKA_PASSWORD=...
    uv run --project tests/benchmark tests/benchmark/capture.py \\
        --survey lsst --night 20261115 \\
        --bootstrap rubin-alert-stream-bootstrap.slac.stanford.edu:9094 \\
        --group-id <your consumer group> \\
        --from 2026-11-15T22:00:00Z --until 2026-11-16T11:00:00Z \\
        --out ~/boom-bench/bundles/lsst-20261115

Start it before the night begins with --from/--until bracketing the night, or
afterwards while the topic still retains it. Use a consumer group of your own:
the capture must not take partitions from a production consumer.
"""

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import OFFSET_END, Consumer, KafkaError, TopicPartition

from benchlib import normalize_survey
from benchlib.bundle import BundleWriter, SchemaRegistry, describe_packet, topic_for

DEFAULT_TOPICS = {"LSST": "lsst-alerts-v11"}
DEFAULT_REGISTRY = "https://rubin-alert-schemas.slac.stanford.edu/schema-registry"


def parse_time_ms(value: str | None) -> int | None:
    if value is None:
        return None
    if value.isdigit():
        return int(value) * 1000
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() * 1000)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--survey", required=True)
    p.add_argument("--night", required=True, help="night label YYYYMMDD used for replay topic names")
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic", default=None, help="source topic (default: the survey's production topic)")
    p.add_argument("--group-id", required=True)
    p.add_argument("--security-protocol", default=None,
                   help="default: SASL_PLAINTEXT when credentials are set, as BOOM does, else PLAINTEXT")
    p.add_argument("--from", dest="from_", default=None, help="ISO time or Unix seconds; default: now")
    p.add_argument("--until", default=None, help="ISO time or Unix seconds; stop at the first packet past it")
    p.add_argument("--idle-timeout", type=float, default=1800.0,
                   help="stop after this many seconds without a packet (default 30 min)")
    p.add_argument("--max-packets", type=int, default=None)
    p.add_argument("--schema-registry", default=DEFAULT_REGISTRY, help="for Confluent-framed packets (LSST)")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()

    survey = normalize_survey(args.survey)
    topic = args.topic or DEFAULT_TOPICS.get(survey) or topic_for(survey, args.night)
    username = os.environ.get("CAPTURE_KAFKA_USERNAME")
    password = os.environ.get("CAPTURE_KAFKA_PASSWORD")
    protocol = args.security_protocol or ("SASL_PLAINTEXT" if username and password else "PLAINTEXT")
    conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "security.protocol": protocol,
        "fetch.message.max.bytes": 50 * 1024 * 1024,
    }
    if "SASL" in protocol:
        conf.update({"sasl.mechanisms": "SCRAM-SHA-512", "sasl.username": username, "sasl.password": password})

    consumer = Consumer(conf)
    meta = consumer.list_topics(topic, timeout=60)
    if topic not in meta.topics or meta.topics[topic].error is not None:
        print(f"topic {topic} not found on {args.bootstrap}", file=sys.stderr)
        return 1
    partitions = sorted(meta.topics[topic].partitions)
    from_ms = parse_time_ms(args.from_)
    until_ms = parse_time_ms(args.until)
    if from_ms is not None:
        offsets = consumer.offsets_for_times([TopicPartition(topic, p, from_ms) for p in partitions], timeout=60)
    else:
        offsets = [TopicPartition(topic, p, OFFSET_END) for p in partitions]
    consumer.assign(offsets)

    registry = SchemaRegistry(args.schema_registry, cache_dir=args.out / "schemas")
    writer = BundleWriter(args.out, survey, args.night, source=f"{args.bootstrap}/{topic}", overwrite=args.overwrite)
    replay_topic = topic_for(survey, args.night)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    done_partitions: set[int] = set()
    last_packet = time.time()
    undecoded = 0
    try:
        while not stopping:
            if args.max_packets is not None and writer.count >= args.max_packets:
                break
            if until_ms is not None and len(done_partitions) == len(partitions):
                break
            if time.time() - last_packet > args.idle_timeout:
                print("idle timeout reached")
                break
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            _, ts_ms = msg.timestamp()
            if until_ms is not None and ts_ms >= until_ms:
                done_partitions.add(msg.partition())
                continue
            last_packet = time.time()
            payload = msg.value()
            try:
                info = describe_packet(payload, survey, registry)
            except Exception as e:
                # Kept: the packet is still replayable; only its latency cannot be joined.
                undecoded += 1
                if undecoded <= 10:
                    print(f"could not decode packet at {msg.partition()}:{msg.offset()}: {e}", file=sys.stderr)
                info = {"alert_id": -1, "object_id": "", "ra": "", "dec": "", "t_obs_jd": None, "programid": None}
            writer.add(payload, topic=topic_for(survey, args.night, info["programid"]) if survey == "ZTF"
                       else replay_topic, t_kafka_ms=ts_ms, **info)
            if writer.count % 10000 == 0:
                print(f"{writer.count} packets captured (latest broker time "
                      f"{datetime.fromtimestamp(ts_ms / 1000, timezone.utc):%Y-%m-%dT%H:%M:%SZ})")
    finally:
        writer.close({"clock": "kafka", "source_topic": topic, "undecoded": undecoded,
                      "from_ms": from_ms, "until_ms": until_ms})
        consumer.close()
    print(f"captured {writer.count} packets to {args.out} ({undecoded} undecoded)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
