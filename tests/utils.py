import os
import socket

from kafka import TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (
    KafkaTimeoutError,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)


def get_kafka_address():
    """Get the kafka (host, port) for the testing environment"""
    broker_hostname = os.environ.get("POPPY_TEST_KAFKA_HOSTNAME", "127.0.0.1")
    broker_port = int(os.environ.get("POPPY_TEST_KAFKA_PORT", "9092"))
    return broker_hostname, broker_port


def get_kafka_servers():
    """Get the kafka confic for the testing environment"""
    broker_hostname, broker_port = get_kafka_address()
    return f"{broker_hostname}:{broker_port}"


def check_kafka_connection():
    """Connectivity check for kafka admin client"""
    try:
        with socket.create_connection(get_kafka_address(), timeout=5):
            pass
    except OSError:
        return False

    try:
        client = KafkaAdminClient(
            bootstrap_servers=get_kafka_servers(),
            client_id="poppy-test-client",
            bootstrap_timeout_ms=5000,
        )
    except KafkaTimeoutError:
        return False
    client.close()
    return True


def delete_kafka_topic(admin_client, topic):
    """Delete kafka topic"""
    try:
        admin_client.delete_topics([topic])
    except UnknownTopicOrPartitionError:
        pass


def create_kafka_topic(admin_client, topic):
    """Create kafka topic"""
    topic_list = []
    topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError:
        pass


def bootstrap_kafka_tests(admin_client, topic):
    """Bootstrap kafka environment for tests"""
    delete_kafka_topic(admin_client, topic)
    create_kafka_topic(admin_client, topic)


def get_kafka_end_offset(tq, topic):
    partition = TopicPartition(topic, 0)
    return tq.engine.consumer.end_offsets([partition])[partition]


def get_kafka_committed_offset(tq, topic):
    partition = TopicPartition(topic, 0)
    return tq.engine.consumer.committed(partition)
