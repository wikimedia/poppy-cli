import json
from poppy.engine import EmptyQueueException
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
        self.assertTrue(
            "Enqueue a message in the key/value format to the queue"
            in help_result.output
        )
        self.assertTrue("--message-entry" in help_result.output)

    def test_cli_enqueue_raw_help(self):
        """Test that enqueue CLI returns a help with the right args."""
        runner = CliRunner()
        help_result = runner.invoke(cli.enqueue_raw, ["--help"])
        self.assertTrue(help_result.exit_code == 0)
        self.assertTrue(self.help_msg in help_result.output)
        self.assertTrue(
            "Enqueue raw json formatted messages to the queue" in help_result.output
        )
        self.assertTrue("--message-input" in help_result.output)
        self.assertTrue("--raise-on-serialization-error" in help_result.output)

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
                    "--message-entry",
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

    def test_cli_unit_enqueue_raw(self):
        """Test that CLI enqueue raw method adds multiple messages properly"""

        with mock.patch("poppy.cli.Queue") as mock_message_queue:
            mock_message_obj = mock.Mock()
            mock_message_queue.return_value = mock_message_obj

            runner = CliRunner()
            with runner.isolated_filesystem():
                with open("raw_input.txt", "w") as f:
                    f.write(
                        '{"raw-key-1": "raw-value-1"}\n{"raw-key-2": "raw-value-2"}\n'
                    )

                runner.invoke(
                    cli.main,
                    [
                        "--broker-url",
                        self.broker_url,
                        "--queue-name",
                        self.queue_name,
                        "enqueue-raw",
                        "--message-input",
                        "raw_input.txt",
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
                mock_message_obj.enqueue.assert_has_calls(
                    [
                        mock.call({"raw-key-1": "raw-value-1"}),
                        mock.call({"raw-key-2": "raw-value-2"}),
                    ]
                )

    def test_cli_unit_enqueue_raw_invalid_json(self):
        """Test that CLI enqueue raw prints error for invalid json input"""

        with mock.patch("poppy.cli.Queue") as mock_message_queue:
            mock_message_obj = mock.Mock()
            mock_message_queue.return_value = mock_message_obj

            runner = CliRunner()
            with runner.isolated_filesystem():
                with open("raw_input.txt", "w") as f:
                    f.write('{"valid": "input"}\nwrong:input')

                result = runner.invoke(
                    cli.main,
                    [
                        "--broker-url",
                        self.broker_url,
                        "--queue-name",
                        self.queue_name,
                        "enqueue-raw",
                        "--message-input",
                        "raw_input.txt",
                    ],
                    obj={},
                )

            self.assertTrue(result.exit_code == 0)
            expected_output = [
                "Cannot decode JSON: 'wrong:input'\n",
                "Expecting value: line 1 column 1 (char 0)\n",
            ]
            self.assertEqual(result.output, "".join(expected_output))

    def test_cli_unit_enqueue_raw_invalid_json_raises(self):
        """Test that CLI enqueue raw prints error for invalid json input"""

        with mock.patch("poppy.cli.Queue") as mock_message_queue:
            mock_message_obj = mock.Mock()
            mock_message_queue.return_value = mock_message_obj

            runner = CliRunner()
            with runner.isolated_filesystem():
                with open("raw_input.txt", "w") as f:
                    f.write('{"valid": "input"}\nwrong:input')

                result = runner.invoke(
                    cli.main,
                    [
                        "--broker-url",
                        self.broker_url,
                        "--queue-name",
                        self.queue_name,
                        "enqueue-raw",
                        "--message-input",
                        "raw_input.txt",
                        "--raise-on-serialization-error",
                        "True",
                    ],
                    obj={},
                )

            self.assertTrue(result.exit_code == 1)

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

    def test_cli_unit_dequeue_batch(self):
        """Test that CLI dequeue method pops batch messages properly"""

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
                    "--batch",
                    "10",
                ],
                obj={},
            )
            mock_message_queue.assert_called_once_with(self.config)
            self.assertEqual(mock_message_obj.dequeue.call_count, 10)

    def test_cli_unit_dequeue_until_empty(self):
        """Test that CLI dequeue method pops batch messages properly"""

        with mock.patch("poppy.cli.Queue") as mock_message_queue:
            mock_message_obj = mock.Mock()
            mock_message_obj.dequeue.side_effect = [
                EmptyQueueException(),
            ]
            mock_message_queue.return_value = mock_message_obj
            runner = CliRunner()
            result = runner.invoke(
                cli.main,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "dequeue",
                    "--exit-on-empty",
                    "True",
                    "--dequeue-raise-on-empty",
                    "True",
                    "--blocking-dequeue-timeout",
                    "5",
                ],
                obj={},
            )

            config = self.config
            config["DEQUEUE_EXIT_ON_EMPTY"] = True
            config["DEQUEUE_TIMEOUT"] = 5
            config["RAISE_ON_EMPTY_DEQUEUE"] = True

            mock_message_queue.assert_called_once_with(self.config)
            self.assertEqual(mock_message_obj.dequeue.call_count, 1)
            self.assertEqual(result.exit_code, 100)

    def test_cli_unit_dequeue_batch_until_empty(self):
        """Test that CLI dequeue method pops batch messages properly"""

        with mock.patch("poppy.cli.Queue") as mock_message_queue:
            mock_message_obj = mock.Mock()
            mock_message_obj.dequeue.side_effect = [
                mock_message_obj,
                mock_message_obj,
                mock_message_obj,
                EmptyQueueException(),
            ]
            mock_message_queue.return_value = mock_message_obj
            runner = CliRunner()
            result = runner.invoke(
                cli.main,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "dequeue",
                    "--batch",
                    "10",
                    "--exit-on-empty",
                    "True",
                    "--dequeue-raise-on-empty",
                    "True",
                    "--blocking-dequeue-timeout",
                    "5",
                ],
                obj={},
            )
            config = self.config
            config["DEQUEUE_EXIT_ON_EMPTY"] = True
            config["DEQUEUE_TIMEOUT"] = 5
            config["RAISE_ON_EMPTY_DEQUEUE"] = True
            mock_message_queue.assert_called_once_with(self.config)
            self.assertEqual(mock_message_obj.dequeue.call_count, 4)
            self.assertEqual(result.exit_code, 100)


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

    def test_cli_integration_enqueues_raw(self):
        """Test that CLI enqueues raw messages from CLI options"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)
        runner = CliRunner()

        with runner.isolated_filesystem():
            with open("raw_input.txt", "w") as f:
                f.write('{"raw-key-1": "raw-value-1"}\n{"raw-key-2": "raw-value-2"}\n')

            result = runner.invoke(
                cli.main,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "enqueue-raw",
                    "--message-input",
                    "raw_input.txt",
                ],
                obj={},
            )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(self.tq.engine.queue.qsize(), 2)

        result = self.tq.dequeue()
        self.assertEqual(result, b'{"raw-key-1": "raw-value-1"}')
        result = self.tq.dequeue()
        self.assertEqual(result, b'{"raw-key-2": "raw-value-2"}')

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
                "--message-entry",
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
                "--message-entry",
                "k1",
                "v1",
                "--message-entry",
                "k2",
                "v2",
                "--message-entry",
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

    def test_cli_integration_dequeues_batch(self):
        """Test that CLI dequeues batched message from CLI"""

        self.assertEqual(self.tq.engine.queue.qsize(), 0)
        for i in range(3):
            message = {f"cli-input-key-{i}": f"cli-input-value-{i}"}
            self.tq.enqueue(message)

        self.assertEqual(self.tq.engine.queue.qsize(), 3)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "dequeue",
                "--batch",
                "3",
            ],
            obj={},
        )

        self.assertTrue(result.exit_code == 0)

        expected_messages = [
            '{"cli-input-key-0": "cli-input-value-0"}\n',
            '{"cli-input-key-1": "cli-input-value-1"}\n',
            '{"cli-input-key-2": "cli-input-value-2"}\n',
        ]
        self.assertEqual(result.output, "".join(expected_messages))

    def test_cli_integration_dequeues_empty(self):
        """Test that CLI dequeues empty message from CLI"""

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
                "--blocking-dequeue-timeout",
                "10",
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

    def test_cli_integration_enqueues_raw(self):
        """Test that CLI enqueues raw messages from CLI options"""

        tq = Queue(self.config)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)

        runner = CliRunner()

        with runner.isolated_filesystem():
            with open("raw_input.txt", "w") as f:
                f.write('{"raw-key-1": "raw-value-1"}\n{"raw-key-2": "raw-value-2"}\n')

            result = runner.invoke(
                cli.main,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "enqueue-raw",
                    "--message-input",
                    "raw_input.txt",
                ],
                obj={},
            )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 2)

        result = tq.dequeue()
        self.assertEqual(result, b'{"raw-key-1": "raw-value-1"}')
        result = tq.dequeue()
        self.assertEqual(result, b'{"raw-key-2": "raw-value-2"}')

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
                "--message-entry",
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
                "--message-entry",
                "k1",
                "v1",
                "--message-entry",
                "k2",
                "v2",
                "--message-entry",
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

    def test_cli_integration_dequeues_batch(self):
        """Test that CLI dequeues batched message from CLI"""
        tq = Queue(self.config)

        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 0)
        for i in range(3):
            message = {f"cli-input-key-{i}": f"cli-input-value-{i}"}
            tq.enqueue(message)
        tq.engine.producer.close()

        self.assertEqual(get_kafka_end_offset(tq, self.queue_name), 3)

        runner = CliRunner()
        result = runner.invoke(
            cli.main,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "dequeue",
                "--batch",
                "3",
            ],
            obj={},
        )

        self.assertTrue(result.exit_code == 0)

        expected_messages = [
            '{"cli-input-key-0": "cli-input-value-0"}\n',
            '{"cli-input-key-1": "cli-input-value-1"}\n',
            '{"cli-input-key-2": "cli-input-value-2"}\n',
        ]
        self.assertEqual(result.output, "".join(expected_messages))

    def test_cli_integration_dequeues_empty(self):
        """Test that CLI dequeues empty message from CLI"""
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
                "--blocking-dequeue-timeout",
                "10",
            ],
            obj={},
        )
        self.assertTrue(result.exit_code == 0)
