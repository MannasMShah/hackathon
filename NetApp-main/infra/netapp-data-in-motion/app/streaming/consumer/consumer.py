from kafka import KafkaConsumer
import json, os, time, requests

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor_stream")
API_ENDPOINT = os.getenv("STREAM_API", "http://localhost:8001/stream/event")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=0,
)

print(f"[consumer] <- {BOOTSTRAP} topic={TOPIC}")
for msg in consumer:
    evt = msg.value
    print("[consumer] recv:", evt)
    # best-effort forward to stream API
    try:
        r = requests.post(API_ENDPOINT, json=evt, timeout=2.0)
        if r.status_code >= 300:
            print("[consumer] backend error:", r.status_code, r.text)
    except Exception as e:
        print("[consumer] backend offline, will keep consuming. Err:", e)
