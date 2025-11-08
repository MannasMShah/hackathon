from fastapi import FastAPI
from pydantic import BaseModel
from collections import defaultdict
from typing import Dict, Any, List
from datetime import datetime
app = FastAPI(title='NetApp Stream API', version='1.0')
events: List[Dict[str, Any]] = []
hotness = defaultdict(int)
HOT_THRESHOLD = 20
TEMP_ALERT = 80.0
class StreamEvent(BaseModel):
    event_id: int
    device_id: int
    temperature: float
    bytes: int
    timestamp: float
@app.get('/health')
def health():
    return {'ok': True, 'events': len(events)}
@app.post('/stream/event')
def stream_event(e: StreamEvent):
    events.append(e.model_dump())
    hotness[e.device_id] += 1
    actions = []
    if hotness[e.device_id] >= HOT_THRESHOLD:
        actions.append({'action': 'tier_to_hot', 'device_id': e.device_id, 'reason': 'high_access_frequency'})
    if e.temperature >= TEMP_ALERT:
        actions.append({'action': 'raise_alert', 'device_id': e.device_id, 'reason': 'over_temperature'})
    return {'received_at': datetime.utcnow().isoformat() + 'Z',
            'queued_events': len(events),
            'device_hotness': hotness[e.device_id],
            'actions': actions}
@app.get('/stream/peek')
def peek(n: int = 10):
    return events[-n:]
