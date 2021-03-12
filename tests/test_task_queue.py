#!/usr/bin/env python

"""Tests for `poppy` package."""

import unittest
from contextlib import closing
from unittest import mock

from poppy.task import TaskQueue


class TestTaskQueueUnit(unittest.TestCase):
    """Unit tests for poppy TaskQueue"""

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

        task = {"key": "value"}
        msg = mock.Mock()
        msg.payload = task
        tq = TaskQueue(self.config)
        tq.engine.queue.get.return_value = msg
        result = tq.dequeue()
        tq.engine.queue.get.assert_called_once_with(
            block=self.config["BLOCKING_DEQUEUE"],
            timeout=self.config["DEQUEUE_TIMEOUT"],
        )
        self.assertDictEqual(result, task)


class TestTaskQueueIntegration(unittest.TestCase):
    """Integration test for poppy TaskQueue"""

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
        self.assertEqual(result, task)
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
        self.assertEqual(result, tasks[0])
        self.assertEqual(self.tq.engine.queue.qsize(), 3)

        result = self.tq.dequeue()
        self.assertEqual(result, tasks[1])
        self.assertEqual(self.tq.engine.queue.qsize(), 2)
