from kafka import KafkaProducer
import json, time, random, os

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor_stream")
DELAY = float(os.getenv("EVENT_DELAY_SECONDS", "1.0"))

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
)

print(f"[producer] -> {BOOTSTRAP} topic={TOPIC}")
i = 0
while True:
    i += 1
    evt = {
        "event_id": i,
        "device_id": random.randint(1, 8),
        "temperature": round(random.uniform(20, 95), 2),
        "bytes": random.randint(10_000, 5_000_000),
        "timestamp": time.time(),
    }
    producer.send(TOPIC, value=evt)
    print("[producer] sent:", evt)
    time.sleep(DELAY)
