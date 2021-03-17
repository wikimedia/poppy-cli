import json
import unittest
from copy import deepcopy
from unittest import mock

from click.testing import CliRunner
from kafka.admin import KafkaAdminClient

from poppy import cli
from poppy.messsaging import Queue

from .utils import (
    bootstrap_kafka_tests,
    check_kafka_connection,
    delete_kafka_topic,
    get_kafka_end_offset,
    get_kafka_servers,
)

CONFIG_TEST_IN_MEMORY_URL = "memory://"


class TestCLIUnit(unittest.TestCase):
    """Tests for poppy CLI"""

    def setUp(self):
        self.broker_url = CONFIG_TEST_IN_MEMORY_URL
        self.queue_name = "test-message-queue"
        self.config = deepcopy(Queue.get_default_config())
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name
        self.help_msg = "Show this message and exit."

    def test_cli_main_help(self):
        """Test that main CLI group returns a help with the right args."""
        runner = CliRunner()
        help_result = runner.invoke(cli.main, ["--help"])
        self.assertTrue(help_result.exit_code == 0)
        self.assertTrue(self.help_msg in help_result.output)

    def test_cli_enqueue_help(self):
        """Test that enqueue CLI returns a help with the right args."""
        runner = CliRunner()
        help_result = runner.invoke(cli.enqueue, ["--help"])
        self.assertTrue(help_result.exit_code == 0)
        self.assertTrue(self.help_msg in help_result.output)
        self.assertTrue("Enqueue a message to the queue" in help_result.output)
        self.assertTrue("--message-meta" in help_result.output)

    def test_cli_dequeue_help(self):
        """Test that dequeue CLI returns a help with the right args."""
        runner = CliRunner()
        help_result = runner.invoke(cli.dequeue, ["--help"])
        self.assertTrue(help_result.exit_code == 0)
        self.assertTrue(self.help_msg in help_result.output)
        self.assertTrue("Dequeue message from the queue" in help_result.output)

    def test_cli_unit_enqueue(self):
        """Test that CLI enqueue method adds messages properly"""

        with mock.patch("poppy.cli.Queue") as mock_message_queue:
            mock_message_obj = mock.Mock()
            mock_message_queue.return_value = mock_message_obj
            runner = CliRunner()
            runner.invoke(
                cli.main,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "enqueue",
                    "--message-meta",
                    "cli-input-key",
                    "cli-input-value",
                ],
                obj={},
            )
            mock_message_queue.assert_called_once_with(
                {
                    "BROKER_URL": self.broker_url,
                    "QUEUE_NAME": "test-message-queue",
                    "CONNECTION_TIMEOUT": 5,
                }
            )
            mock_message_obj.enqueue.assert_called_once_with(
                {"cli-input-key": "cli-input-value"}
            )

    def test_cli_unit_dequeue(self):
        """Test that CLI dequeue method pops messages properly"""

        with mock.patch("poppy.cli.Queue") as mock_message_queue:
            mock_message_obj = mock.Mock()
            mock_message_queue.return_value = mock_message_obj
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
            mock_message_queue.assert_called_once_with(self.config)
            mock_message_obj.dequeue.assert_called_once_with()


class TestCLIIntegrationKombu(unittest.TestCase):
    """Integration tests for poppy CLI backed by kombu"""

    def setUp(self):
        self.broker_url = CONFIG_TEST_IN_MEMORY_URL
        self.queue_name = "test-message-queue"
        self.config = deepcopy(Queue.get_default_config())
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name
        self.tq = Queue(self.config)

    def tearDown(self):
        self.tq.engine.queue.queue.delete()
        self.tq.close()

    def test_cli_integration_enqueues_message(self):
        """Test that CLI enqueues message from CLI options"""

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
                "--message-meta",
                "cli-input-key",
                "cli-input-value",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(self.tq.engine.queue.qsize(), 1)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"cli-input-key": "cli-input-value"}')

    def test_cli_integration_enqueues_message_multiple_keys(self):
        """Test that CLI enqueues message from CLI options"""

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
                "--message-meta",
                "k1",
                "v1",
                "--message-meta",
                "k2",
                "v2",
                "--message-meta",
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

    def test_cli_integration_dequeues_message(self):
        """Test that CLI dequeues message from CLI"""

        message = {"cli-input-key": "cli-input-value"}
        self.assertEqual(self.tq.engine.queue.qsize(), 0)
        self.tq.enqueue(message)
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
        self.assertDictEqual(result, message)

    def test_cli_integration_dequeues_empty(self):
        """Test that CLI dequeues message from CLI"""

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
        self.config = deepcopy(Queue.get_default_config())
        self.config["BROKER_URL"] = self.broker_url
        self.config["QUEUE_NAME"] = self.queue_name
        self.config["CONSUMER_GROUP_ID"] = self.id()

    def tearDown(self):
        delete_kafka_topic(self.admin_client, self.queue_name)

    def test_cli_integration_enqueues_message(self):
        """Test that CLI enqueues message from CLI options"""
        tq = Queue(self.config)
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
                "--message-meta",
                "cli-input-key",
                "cli-input-value",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)

        result = tq.dequeue()
        self.assertEqual(result, b'{"cli-input-key": "cli-input-value"}')

    def test_cli_integration_enqueues_message_multiple_keys(self):
        """Test that CLI enqueues message from CLI options"""
        tq = Queue(self.config)
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
                "--message-meta",
                "k1",
                "v1",
                "--message-meta",
                "k2",
                "v2",
                "--message-meta",
                "k3",
                "v3",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 1)

        result = tq.dequeue()
        self.assertEqual(result, b'{"k1": "v1", "k2": "v2", "k3": "v3"}')

    def test_cli_integration_dequeues_message(self):
        """Test that CLI dequeues message from CLI"""
        tq = Queue(self.config)
        message = {"cli-input-key": "cli-input-value"}
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)
        tq.enqueue(message)
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
        self.assertEqual({"cli-input-key": "cli-input-value"}, message)

    def test_cli_integration_dequeues_empty(self):
        """Test that CLI dequeues message from CLI"""
        tq = Queue(self.config)
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
