from kafka import KafkaProducer
import json
import time


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


for i in range(10):
    transaction = {
        "transaction_id": f"tx_{i}",
        "user_id": f"user_{i}",
        "amount": i * 100,
        "country": "TH"
    }

    producer.send(
        "transactions_raw",
        transaction
    )

    print("sent:", transaction)

    time.sleep(1)


producer.flush()
producer.close()