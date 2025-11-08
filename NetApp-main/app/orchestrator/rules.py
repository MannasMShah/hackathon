def decide_tier(access_freq_per_day: int, latency_sla_ms: int) -> str:
    """
    Very simple rule engine:
    - Hot: very frequent access or tight SLA
    - Warm: moderate access
    - Cold: infrequent access and loose SLA
    """
    if access_freq_per_day >= 100 or latency_sla_ms <= 25:
        return "hot"
    if access_freq_per_day >= 10 or latency_sla_ms <= 120:
        return "warm"
    return "cold"
