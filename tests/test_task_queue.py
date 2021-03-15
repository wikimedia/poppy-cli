#!/usr/bin/env python

"""Tests for `poppy` package."""

import unittest
from contextlib import closing
from unittest import mock

from kafka.admin import KafkaAdminClient

from poppy.engine import KafkaEngine
from poppy.task import TaskQueue

from .utils import (
    bootstrap_kafka_tests,
    check_kafka_connection,
    delete_kafka_topic,
    get_kafka_committed_offset,
    get_kafka_end_offset,
    get_kafka_servers,
)


class TestTaskQueueKombuUnit(unittest.TestCase):
    """Unit tests for poppy TaskQueue with kombu backend"""

    def setUp(self):
        self.broker_url = "memory://"
        self.queue_name = "test-task-queue"

        self.config = TaskQueue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_task_queue_unit_init(self, mock_connection):
        """Test that TaskQueue attributes are set properly"""

        mock_conn_obj = mock.Mock()
        mock_connection.return_value = mock_conn_obj

        tq = TaskQueue(self.config)
        self.assertEqual(tq.engine.name, self.queue_name)
        self.assertEqual(tq.engine.broker_url, self.broker_url)
        mock_connection.assert_called_once_with(
            self.broker_url, connect_timeout=self.config["CONNECTION_TIMEOUT"]
        )
        mock_conn_obj.SimpleQueue.assert_called_once_with(
            self.queue_name, serializer="json"
        )

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_task_queue_unit_close(self, mock_connection):
        """Test that TaskQueue connections are closed properly"""

        mock_conn_obj = mock.Mock()
        mock_connection.return_value = mock_conn_obj

        with closing(TaskQueue(self.config)) as tq:
            tq.engine.queue.close.assert_not_called()
            tq.engine.conn.release.assert_not_called()

        tq.engine.queue.close.assert_called_once()
        tq.engine.conn.release.assert_called_once()

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_task_queue_unit_enqueue(self, mock_connection):
        """Test that TaskQueue mock enqueues task"""

        task = {"key": "value"}
        tq = TaskQueue(self.config)
        tq.enqueue(task)
        tq.engine.queue.put.assert_called_once_with(task)

    @mock.patch("poppy.engine.Connection", autospec=True)
    def test_task_queue_unit_dequeue(self, mock_connection):
        """Test that TaskQueue mock dequeues task"""

        msg = mock.Mock()
        msg.body = b'{"key": "value"}'
        tq = TaskQueue(self.config)
        tq.engine.queue.get.return_value = msg
        result = tq.dequeue()
        tq.engine.queue.get.assert_called_once_with(
            block=self.config["BLOCKING_DEQUEUE"],
            timeout=self.config["DEQUEUE_TIMEOUT"],
        )
        self.assertEqual(result, b'{"key": "value"}')


class TestTaskQueueKombuIntegration(unittest.TestCase):
    """Integration test for poppy TaskQueue with kombu backend"""

    def setUp(self):
        self.broker_url = "memory://"
        self.queue_name = "test-task-queue"
        self.config = TaskQueue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name
        self.tq = TaskQueue(self.config)

    def tearDown(self):
        self.tq.engine.queue.queue.delete()
        self.tq.close()

    def test_task_queue_integration_connection(self):
        """Test that TaskQueue connects properly"""

        with closing(TaskQueue(self.config)) as tq:
            self.assertTrue(tq.engine.conn.connected)

        self.assertFalse(tq.engine.conn.connected)

    def test_task_queue_integration_enqueue_single_task(self):
        """Test that TaskQueue enqueues single task properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        task = {"key": "value"}
        self.tq.enqueue(task)

        self.assertEqual(self.tq.engine.queue.qsize(), 1)

    def test_task_queue_integration_enqueue_multiple_tasks(self):
        """Test that TaskQueue enqueues multiple tasks properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        tasks = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]
        for task in tasks:
            self.tq.enqueue(task)

        self.assertEqual(self.tq.engine.queue.qsize(), 4)

    def test_task_queue_integration_dequeue_single_task(self):
        """Test that TaskQueue dequeues single task properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        task = {"key": "value"}
        self.tq.enqueue(task)
        self.assertEqual(self.tq.engine.queue.qsize(), 1)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"key": "value"}')
        self.assertEqual(self.tq.engine.queue.qsize(), 0)

    def test_task_queue_integration_dequeue_multiple_tasks(self):
        """Test that TaskQueue dequeues multiple task properly"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        tasks = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]

        for task in tasks:
            self.tq.enqueue(task)

        self.assertEqual(self.tq.engine.queue.qsize(), 4)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"key1": "value1"}')
        self.assertEqual(self.tq.engine.queue.qsize(), 3)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"key2": "value2"}')
        self.assertEqual(self.tq.engine.queue.qsize(), 2)


class TestTaskQueueKafkaUnit(unittest.TestCase):
    """Unit tests for poppy TaskQueue with kafka backend"""

    def setUp(self):
        self.broker_url = "kafka://kafka_hostname1;kafka_hostname2"
        self.queue_name = "test-task-queue"

        self.config = TaskQueue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_task_queue_unit_init(self, mock_producer, mock_consumer):
        """Test that TaskQueue attributes are set properly"""

        tq = TaskQueue(self.config)
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
            group_id="poppy-test-task-queue",
            consumer_timeout_ms=5000,
        )

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_task_queue_unit_close(self, mock_producer, mock_consumer):
        """Test that TaskQueue connections are closed properly"""

        with closing(TaskQueue(self.config)) as tq:
            tq.engine.consumer.commit.assert_not_called()
            tq.engine.consumer.close.assert_not_called()
            tq.engine.producer.close.assert_not_called()

        tq.engine.consumer.commit.assert_called_once()
        tq.engine.consumer.close.assert_called_once()
        tq.engine.producer.close.assert_called_once()

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_task_queue_unit_enqueue(self, mock_producer, mock_consumer):
        """Test that TaskQueue mock enqueues task"""

        task = {"key": "value"}
        tq = TaskQueue(self.config)
        tq.enqueue(task)
        tq.engine.producer.send.assert_called_once_with(self.queue_name, value=task)

    @mock.patch("poppy.engine.KafkaConsumer", autospec=True)
    @mock.patch("poppy.engine.KafkaProducer", autospec=True)
    def test_task_queue_unit_dequeue(self, mock_producer, mock_consumer):
        """Test that TaskQueue mock dequeues task"""

        tq = TaskQueue(self.config)
        task = {"key": "value"}
        msg = mock.Mock()
        msg.value = task
        tq.engine.consumer.__next__.return_value = msg
        result = tq.dequeue()
        tq.engine.consumer.__next__.assert_called_once()
        self.assertDictEqual(result, task)


@unittest.skipIf(check_kafka_connection() is False, "Kafka connection unavailable")
class TestTaskQueueKafkaIntegration(unittest.TestCase):
    """Integration test for poppy TaskQueue with kafka backend"""

    def setUp(self):
        self.queue_name = "test-poppy"
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=get_kafka_servers(), client_id=self.__class__
        )

        bootstrap_kafka_tests(self.admin_client, self.queue_name)
        self.broker_url = f"kafka://{get_kafka_servers()}"
        self.config = TaskQueue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name
        self.config["DEQUEUE_TIMEOUT"] = 10

    def tearDown(self):
        delete_kafka_topic(self.admin_client, self.queue_name)

    def test_task_queue_integration_enqueue_single_task(self):
        """Test that TaskQueue enqueues single task properly"""
        tq = TaskQueue(self.config)
        task = {"key": "value"}
        tq.enqueue(task)
        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)

    def test_task_queue_integration_enqueue_multiple_tasks(self):
        """Test that TaskQueue enqueues multiple tasks properly"""
        tq = TaskQueue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        tasks = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]
        for task in tasks:
            tq.enqueue(task)

        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 4)

    def test_task_queue_integration_dequeue_single_task(self):
        """Test that TaskQueue dequeues single task properly"""
        tq = TaskQueue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)
        task = {"key": "value"}
        tq.enqueue(task)
        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)
        result = tq.dequeue()
        tq.engine.consumer.commit()
        self.assertEqual(result, b'{"key": "value"}')
        self.assertEqual(get_kafka_committed_offset(tq, self.queue_name), 1)

    def test_task_queue_integration_dequeue_multiple_tasks(self):
        """Test that TaskQueue dequeues multiple task properly"""
        tq = TaskQueue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        tasks = [
            {"key1": "value1"},
            {"key2": "value2"},
            {"key3": "value3"},
            {"key4": "value4"},
        ]

        for task in tasks:
            tq.enqueue(task)

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
