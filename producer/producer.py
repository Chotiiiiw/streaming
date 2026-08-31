import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate transactions and publish them to Kafka."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of transactions to generate.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between transactions.",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=5,
        help="Number of unique users to generate.",
    )
    return parser.parse_args()


def create_transaction(index, user_count):
    return {
        "transaction_id": f"tx_{uuid.uuid4().hex}",
        "user_id": f"user_{index % user_count}",
        "amount": (index + 1) * 100,
        "country": "TH",
        "event_time": datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    }


def main():
    args = parse_args()

    if args.count <= 0:
        raise ValueError("--count must be greater than zero")

    if args.interval < 0:
        raise ValueError("--interval cannot be negative")

    if args.users <= 0:
        raise ValueError("--users must be greater that zero")
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        for index in range(args.count):
            transaction = create_transaction(index, args.users)
            message_key = transaction["user_id"]
            metadata = producer.send(
                topic="transactions_raw",
                key=message_key,
                value=transaction,
            ).get(timeout=10)

            print(
                f"acknowledged: "
                f"topic={metadata.topic}, "
                f"partition={metadata.partition}, "
                f"offset={metadata.offset}, "
                f"transaction={transaction}"
                f"key={message_key}"
            )

            time.sleep(args.interval)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
