#!/usr/bin/env python

"""Tests for `poppy` package."""

import unittest
from contextlib import closing
from unittest import mock

from kafka.admin import KafkaAdminClient

from poppy.engine import KafkaEngine
from poppy.messsaging import Queue

from .utils import (
    bootstrap_kafka_tests,
    check_kafka_connection,
    delete_kafka_topic,
    get_kafka_committed_offset,
    get_kafka_end_offset,
    get_kafka_servers,
)


class TestQueueKombuUnit(unittest.TestCase):
    """Unit tests for poppy Queue with kombu backend"""

    def setUp(self):
        self.broker_url = "memory://"
        self.queue_name = "test-message-queue"

        self.config = Queue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_message_queue_unit_init(self, mock_connection):
        """Test that Queue attributes are set properly"""

        mock_conn_obj = mock.Mock()
        mock_connection.return_value = mock_conn_obj

        tq = Queue(self.config)
        self.assertEqual(tq.engine.name, self.queue_name)
        self.assertEqual(tq.engine.broker_url, self.broker_url)
        mock_connection.assert_called_once_with(
            self.broker_url, connect_timeout=self.config["CONNECTION_TIMEOUT"]
        )
        mock_conn_obj.SimpleQueue.assert_called_once_with(
            self.queue_name, serializer="json"
        )

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_message_queue_unit_close(self, mock_connection):
        """Test that Queue connections are closed properly"""

        mock_conn_obj = mock.Mock()
        mock_connection.return_value = mock_conn_obj

        with closing(Queue(self.config)) as tq:
            tq.engine.queue.close.assert_not_called()
            tq.engine.conn.release.assert_not_called()

        tq.engine.queue.close.assert_called_once()
        tq.engine.conn.release.assert_called_once()

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_message_queue_unit_enqueue(self, mock_connection):
        """Test that Queue mock enqueues message"""

        message = {"key": "value"}
        tq = Queue(self.config)
        tq.enqueue(message)
        tq.engine.queue.put.assert_called_once_with(message)

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_message_queue_unit_dequeue(self, mock_connection):
        """Test that Queue mock dequeues message"""

        msg = mock.Mock()
        msg.body = b'{"key-kombu-dequeue": "value"}'
        tq = Queue(self.config)
        tq.engine.queue.get.return_value = msg
        result = tq.dequeue()
        tq.engine.queue.get.assert_called_once_with(
            block=self.config["BLOCKING_DEQUEUE"],
            timeout=self.config["DEQUEUE_TIMEOUT"],
        )
        self.assertEqual(result, b'{"key-kombu-dequeue": "value"}')


class TestQueueKombuIntegration(unittest.TestCase):
    """Integration test for poppy Queue with kombu backend"""

    def setUp(self):
        self.broker_url = "memory://"
        self.queue_name = "test-message-queue"
        self.config = Queue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name
        self.tq = Queue(self.config)

    def tearDown(self):
        self.tq.engine.queue.queue.delete()
        self.tq.close()

    def test_message_queue_integration_connection(self):
        """Test that Queue connects properly"""

        with closing(Queue(self.config)) as tq:
            self.assertTrue(tq.engine.conn.connected)

        self.assertFalse(tq.engine.conn.connected)

    def test_message_queue_integration_enqueue_single_message(self):
        """Test that Queue enqueues single message properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        message = {"key": "value"}
        self.tq.enqueue(message)

        self.assertEqual(self.tq.engine.queue.qsize(), 1)

    def test_message_queue_integration_enqueue_multiple_messages(self):
        """Test that Queue enqueues multiple messages properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        messages = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]
        for message in messages:
            self.tq.enqueue(message)

        self.assertEqual(self.tq.engine.queue.qsize(), 4)

    def test_message_queue_integration_dequeue_single_message(self):
        """Test that Queue dequeues single message properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        message = {"key-kombu-dequeue-integration": "value"}
        self.tq.enqueue(message)
        self.assertEqual(self.tq.engine.queue.qsize(), 1)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"key-kombu-dequeue-integration": "value"}')
        self.assertEqual(self.tq.engine.queue.qsize(), 0)

    def test_message_queue_integration_dequeue_multiple_messages(self):
        """Test that Queue dequeues multiple message properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        messages = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]

        for message in messages:
            self.tq.enqueue(message)

        self.assertEqual(self.tq.engine.queue.qsize(), 4)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"key1": "value1"}')
        self.assertEqual(self.tq.engine.queue.qsize(), 3)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"key2": "value2"}')
        self.assertEqual(self.tq.engine.queue.qsize(), 2)


class TestQueueKafkaUnit(unittest.TestCase):
    """Unit tests for poppy Queue with kafka backend"""

    def setUp(self):
        self.broker_url = "kafka://kafka_hostname1;kafka_hostname2"
        self.queue_name = "test-message-queue"

        self.config = Queue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_message_queue_unit_init(self, mock_producer, mock_consumer):
        """Test that Queue attributes are set properly"""

        tq = Queue(self.config)
        self.assertEqual(tq.engine.topic, self.queue_name)
        self.assertListEqual(tq.engine.servers, ["kafka_hostname1", "kafka_hostname2"])
        mock_producer.assert_called_once_with(
            bootstrap_servers=["kafka_hostname1", "kafka_hostname2"],
            value_serializer=KafkaEngine.serializer,
        )
        mock_consumer.assert_called_once_with(
            self.queue_name,
            bootstrap_servers=["kafka_hostname1", "kafka_hostname2"],
            auto_offset_reset=self.config["CONSUMER_AUTO_OFFSET_RESET"],
            enable_auto_commit=self.config["CONSUMER_AUTOCOMMIT"],
            group_id="poppy-test-message-queue",
            consumer_timeout_ms=5000,
        )

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_message_queue_unit_close(self, mock_producer, mock_consumer):
        """Test that Queue connections are closed properly"""

        with closing(Queue(self.config)) as tq:
            tq.engine.consumer.commit.assert_not_called()
            tq.engine.consumer.close.assert_not_called()
            tq.engine.producer.close.assert_not_called()

        tq.engine.consumer.commit.assert_called_once()
        tq.engine.consumer.close.assert_called_once()
        tq.engine.producer.close.assert_called_once()

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_message_queue_unit_enqueue(self, mock_producer, mock_consumer):
        """Test that Queue mock enqueues message"""

        message = {"key": "value"}
        tq = Queue(self.config)
        tq.enqueue(message)
        tq.engine.producer.send.assert_called_once_with(self.queue_name, value=message)

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_message_queue_unit_dequeue(self, mock_producer, mock_consumer):
        """Test that Queue mock dequeues message"""

        tq = Queue(self.config)
        message = {"key": "value"}
        msg = mock.Mock()
        msg.value = message
        tq.engine.consumer.__next__.return_value = msg
        result = tq.dequeue()
        tq.engine.consumer.__next__.assert_called_once()
        self.assertDictEqual(result, message)


@unittest.skipIf(check_kafka_connection() is False, "Kafka connection unavailable")
class TestQueueKafkaIntegration(unittest.TestCase):
    """Integration test for poppy Queue with kafka backend"""

    def setUp(self):
        self.queue_name = "test-poppy"
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=get_kafka_servers(), client_id=self.__class__
        )

        bootstrap_kafka_tests(self.admin_client, self.queue_name)
        self.broker_url = f"kafka://{get_kafka_servers()}"
        self.config = Queue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name
        self.config["DEQUEUE_TIMEOUT"] = 10

    def tearDown(self):
        delete_kafka_topic(self.admin_client, self.queue_name)

    def test_message_queue_integration_enqueue_single_message(self):
        """Test that Queue enqueues single message properly"""
        tq = Queue(self.config)
        message = {"key": "value"}
        tq.enqueue(message)
        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)

    def test_message_queue_integration_enqueue_multiple_messages(self):
        """Test that Queue enqueues multiple messages properly"""
        tq = Queue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        messages = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]
        for message in messages:
            tq.enqueue(message)

        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 4)

    def test_message_queue_integration_dequeue_single_message(self):
        """Test that Queue dequeues single message properly"""
        tq = Queue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)
        message = {"key-kafka-dequeue": "value"}
        tq.enqueue(message)
        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)
        result = tq.dequeue()
        tq.engine.consumer.commit()
        self.assertEqual(result, b'{"key-kafka-dequeue": "value"}')
        self.assertEqual(get_kafka_committed_offset(tq, self.queue_name), 1)

    def test_message_queue_integration_dequeue_multiple_messages(self):
        """Test that Queue dequeues multiple message properly"""
        tq = Queue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        messages = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]

        for message in messages:
            tq.enqueue(message)

        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 4)

        result = tq.dequeue()
        tq.engine.consumer.commit()
        self.assertEqual(result, b'{"key1": "value1"}')
        self.assertEqual(get_kafka_committed_offset(tq, self.queue_name), 1)

        result = tq.dequeue()
        tq.engine.consumer.commit()
        self.assertEqual(result, b'{"key2": "value2"}')
        self.assertEqual(get_kafka_committed_offset(tq, self.queue_name), 2)

        result = tq.dequeue()
        tq.engine.consumer.commit()
        self.assertEqual(result, b'{"key3": "value3"}')
        self.assertEqual(get_kafka_committed_offset(tq, self.queue_name), 3)

        result = tq.dequeue()
        tq.engine.consumer.commit()
        self.assertEqual(result, b'{"key4": "value4"}')
        self.assertEqual(get_kafka_committed_offset(tq, self.queue_name), 4)
