import json
import unittest
from unittest import mock

from click.testing import CliRunner
from kafka.admin import KafkaAdminClient

from poppy import cli
from poppy.task import TaskQueue

from .utils import (
    bootstrap_kafka_tests,
    check_kafka_connection,
    delete_kafka_topic,
    get_kafka_end_offset,
    get_kafka_servers,
)


class TestCLIUnit(unittest.TestCase):
    """Tests for poppy CLI"""

    def setUp(self):
        self.broker_url = "memory://"
        self.queue_name = "test-task-queue"
        self.config = TaskQueue.get_default_config()
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name

    def test_cli_main_help(self):
        """Test that main CLI group returns a help with the right args."""
        runner = CliRunner()
        help_result = runner.invoke(cli.main, ["--help"])
        self.assertTrue(help_result.exit_code == 0)
        self.assertTrue("Show this message and exit." in help_result.output)

    def test_cli_enqueue_help(self):
        """Test that enqueue CLI returns a help with the right args."""
        runner = CliRunner()
        help_result = runner.invoke(cli.enqueue, ["--help"])
        self.assertTrue(help_result.exit_code == 0)
        self.assertTrue("Show this message and exit." in help_result.output)
        self.assertTrue("Enqueue a task to the queue" in help_result.output)
        self.assertTrue("--task-meta" in help_result.output)

    def test_cli_dequeue_help(self):
        """Test that dequeue CLI returns a help with the right args."""
        runner = CliRunner()
        help_result = runner.invoke(cli.dequeue, ["--help"])
        self.assertTrue(help_result.exit_code == 0)
        self.assertTrue("Show this message and exit." in help_result.output)
        self.assertTrue("Dequeue task from the queue" in help_result.output)

    def test_cli_unit_enqueue(self):
        """Test that CLI enqueue method adds tasks properly"""

        with mock.patch("poppy.cli.TaskQueue") as mock_task_queue:
            mock_task_obj = mock.Mock()
            mock_task_queue.return_value = mock_task_obj
            runner = CliRunner()
            runner.invoke(
                cli.main,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "enqueue",
                    "--task-meta",
                    "cli-input-key",
                    "cli-input-value",
                ],
                obj={},
            )
            mock_task_queue.assert_called_once_with(
                {
                    "BROKER_URL": "memory://",
                    "QUEUE_NAME": "test-task-queue",
                    "CONNECTION_TIMEOUT": 5,
                }
            )
            mock_task_obj.enqueue.assert_called_once_with(
                {"cli-input-key": "cli-input-value"}
            )

    def test_cli_unit_dequeue(self):
        """Test that CLI dequeue method pops tasks properly"""

        with mock.patch("poppy.cli.TaskQueue") as mock_task_queue:
            mock_task_obj = mock.Mock()
            mock_task_queue.return_value = mock_task_obj
            runner = CliRunner()
            runner.invoke(
                cli.main,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "dequeue",
                ],
                obj={},
            )
            mock_task_queue.assert_called_once_with(self.config)
            mock_task_obj.dequeue.assert_called_once_with()


class TestCLIIntegrationKombu(unittest.TestCase):
    """Integration tests for poppy CLI backed by kombu"""

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

    def test_cli_integration_enqueues_task(self):
        """Test that CLI enqueues task from CLI options"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "enqueue",
                "--task-meta",
                "cli-input-key",
                "cli-input-value",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(self.tq.engine.queue.qsize(), 1)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"cli-input-key": "cli-input-value"}')

    def test_cli_integration_enqueues_task_multiple_keys(self):
        """Test that CLI enqueues task from CLI options"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "enqueue",
                "--task-meta",
                "k1",
                "v1",
                "--task-meta",
                "k2",
                "v2",
                "--task-meta",
                "k3",
                "v3",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(self.tq.engine.queue.qsize(), 1)

        result = self.tq.dequeue()
        self.assertEqual(
            result,
            b'{"k1": "v1", "k2": "v2", "k3": "v3"}',
        )

    def test_cli_integration_dequeues_task(self):
        """Test that CLI dequeues task from CLI"""

        task = {"cli-input-key": "cli-input-value"}
        self.assertEqual(self.tq.engine.queue.qsize(), 0)
        self.tq.enqueue(task)
        self.assertEqual(self.tq.engine.queue.qsize(), 1)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "dequeue",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        result = json.loads(result.output)
        self.assertDictEqual(result, task)

    def test_cli_integration_dequeues_empty(self):
        """Test that CLI dequeues task from CLI"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "dequeue",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)


@unittest.skipIf(check_kafka_connection() is False, "Kafka connection unavailable")
class TestCLIIntegrationKafka(unittest.TestCase):
    """Integration tests for poppy CLI backed by kafka"""

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

    def test_cli_integration_enqueues_task(self):
        """Test that CLI enqueues task from CLI options"""
        tq = TaskQueue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "enqueue",
                "--task-meta",
                "cli-input-key",
                "cli-input-value",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)

        result = tq.dequeue()
        self.assertEqual(result, b'{"cli-input-key": "cli-input-value"}')

    def test_cli_integration_enqueues_task_multiple_keys(self):
        """Test that CLI enqueues task from CLI options"""
        tq = TaskQueue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "enqueue",
                "--task-meta",
                "k1",
                "v1",
                "--task-meta",
                "k2",
                "v2",
                "--task-meta",
                "k3",
                "v3",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)

        result = tq.dequeue()
        self.assertEqual(result, b'{"k1": "v1", "k2": "v2", "k3": "v3"}')

    def test_cli_integration_dequeues_task(self):
        """Test that CLI dequeues task from CLI"""
        tq = TaskQueue(self.config)
        task = {"cli-input-key": "cli-input-value"}
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)
        tq.enqueue(task)
        tq.engine.producer.close()
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "dequeue",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual({"cli-input-key": "cli-input-value"}, task)

    def test_cli_integration_dequeues_empty(self):
        """Test that CLI dequeues task from CLI"""
        tq = TaskQueue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "dequeue",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
