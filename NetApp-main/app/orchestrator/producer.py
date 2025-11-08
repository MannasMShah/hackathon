import os, json, time, sys
from kafka import KafkaProducer

BOOT = os.getenv("KAFKA_BOOTSTRAP","kafka:9092")
TOPIC = os.getenv("TOPIC_ACCESS","access-events")

p = KafkaProducer(bootstrap_servers=BOOT, value_serializer=lambda v: json.dumps(v).encode())
file_id = sys.argv[1] if len(sys.argv) > 1 else "file_001.txt"
msg = {"file_id": file_id, "event":"read", "ts": time.time()}
p.send(TOPIC, msg)
p.flush()
print("sent:", msg)
