from kafka import KafkaConsumer
import json, os, requests, math, time
from collections import defaultdict

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")  # external
TOPIC = os.getenv("KAFKA_TOPIC", "sensor_stream")
API_ENDPOINT = os.getenv("STREAM_API", "http://localhost:8001/stream/event")
Z_THRESH = float(os.getenv("Z_THRESH", "2.5"))

# Welford online stats per device
stats = defaultdict(lambda: {'n':0,'mean':0.0,'M2':0.0})

def update_stats(dev, x):
    s = stats[dev]
    s['n'] += 1
    delta = x - s['mean']
    s['mean'] += delta / s['n']
    delta2 = x - s['mean']
    s['M2'] += delta * delta2
    return s

def zscore(dev, x):
    s = stats[dev]
    if s['n'] < 10:  # warmup
        return 0.0
    var = s['M2'] / (s['n'] - 1) if s['n'] > 1 else 0.0
    sd = math.sqrt(max(var, 1e-9))
    return (x - s['mean']) / sd

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BOOTSTRAP],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

print(f"[consumer] <- {BOOTSTRAP} topic={TOPIC}")
for msg in consumer:
    evt = msg.value
    dev = int(evt.get('device_id', -1))
    temp = float(evt.get('temperature', 0.0))

    # online update + zscore
    update_stats(dev, temp)
    z = zscore(dev, temp)
    evt['anomaly'] = abs(z) > Z_THRESH

    print("[consumer] recv:", evt, "| z=", round(z,2))

    # forward to stream API
    try:
        r = requests.post(API_ENDPOINT, json=evt, timeout=2.0)
        if r.status_code >= 300:
            print("[consumer] backend error:", r.status_code, r.text)
    except Exception as e:
        print("[consumer] backend offline; continuing. Err:", e)
