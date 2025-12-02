#!/usr/bin/env python

import json
import time
from datetime import datetime

from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS


def main():
    print("Kafka bootstrap servers:", KAFKA_BOOTSTRAP_SERVERS)
    print("Kafka topic:", KAFKA_TOPIC_EVENTS)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    for i in range(1000):
        event = {
            "event_time": datetime.utcnow().isoformat(),
            "event_type": "view" if i % 3 else "purchase",
            "user_id": i % 100,
            "user_session": f"session-{i % 50}",
            "product_id": i % 200,
            "category_id": 1,
            "category_code": "cosmetics.nail.polish",
            "brand": "testbrand",
            "price": 10.0,
        }
        key = event["user_session"]
        producer.send(KAFKA_TOPIC_EVENTS, key=key, value=event)

        if i % 100 == 0:
            print(f"Sent {i} events...")
            time.sleep(0.1)

    producer.flush()
    print("Done sending 1000 events.")


if __name__ == "__main__":
    main()