try:
    # kafka-python admin APIs
    from kafka.admin import KafkaAdminClient, NewTopic
except Exception:  # library missing or older
    KafkaAdminClient = None
    NewTopic = None


def ensure_topic(bootstrap: str, topic: str, partitions: int = 1, rf: int = 1):
    """
    Best-effort topic creation. If admin client isn't available or the broker
    auto-creates topics on produce (e.g., Redpanda dev mode), we just no-op.
    """
    if KafkaAdminClient is None or NewTopic is None:
        return  # nothing to do

    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="netapp-admin")
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)])
    except Exception:
        # Topic exists or broker disallows admin ops; safe to ignore for demo.
        pass
