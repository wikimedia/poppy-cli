import json
import unittest
from unittest import mock

from click.testing import CliRunner

from dashi import cli
from dashi.task import TaskQueue


class TestCLIUnit(unittest.TestCase):
    """Tests for dashi CLI"""

    def setUp(self):
        self.broker_url = "memory://"
        self.queue_name = "test-task-queue"

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

        with mock.patch("dashi.cli.TaskQueue") as mock_task_queue:
            mock_task_obj = mock.Mock()
            mock_task_queue.return_value = mock_task_obj
            runner = CliRunner()
            runner.invoke(
                cli.enqueue,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                    "--task-meta",
                    "cli-input-key",
                    "cli-input-value",
                ],
            )
            mock_task_queue.assert_called_once_with(self.queue_name, self.broker_url)
            mock_task_obj.enqueue.assert_called_once_with(
                {"cli-input-key": "cli-input-value"}
            )

    def test_cli_unit_dequeue(self):
        """Test that CLI dequeue method pops tasks properly"""

        with mock.patch("dashi.cli.TaskQueue") as mock_task_queue:
            mock_task_obj = mock.Mock()
            mock_task_queue.return_value = mock_task_obj
            runner = CliRunner()
            runner.invoke(
                cli.dequeue,
                [
                    "--broker-url",
                    self.broker_url,
                    "--queue-name",
                    self.queue_name,
                ],
            )
            mock_task_queue.assert_called_once_with(self.queue_name, self.broker_url)
            mock_task_obj.dequeue.assert_called_once_with()


class TestCLIIntegration(unittest.TestCase):
    """Integration tests for dashi CLI"""

    def setUp(self):
        self.broker_url = "memory://"
        self.queue_name = "test-task-queue"
        self.tq = TaskQueue(self.queue_name, self.broker_url)

    def tearDown(self):
        self.tq.queue.queue.delete()
        self.tq.close()

    def test_cli_integration_enqueues_task(self):
        """Test that CLI enqueues task from CLI options"""

        self.assertEqual(self.tq.queue.qsize(), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.enqueue,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "--task-meta",
                "cli-input-key",
                "cli-input-value",
            ],
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(self.tq.queue.qsize(), 1)

        result = self.tq.dequeue()
        self.assertDictEqual(result, {"cli-input-key": "cli-input-value"})

    def test_cli_integration_enqueues_task_multiple_keys(self):
        """Test that CLI enqueues task from CLI options"""

        self.assertEqual(self.tq.queue.qsize(), 0)

        runner = CliRunner()
        result = runner.invoke(
            cli.enqueue,
            [
                "--broker-url",
                self.broker_url,
                "--queue-name",
                self.queue_name,
                "--task-meta",
                "cli-input-key1",
                "cli-input-value1",
                "--task-meta",
                "cli-input-key2",
                "cli-input-value2",
                "--task-meta",
                "cli-input-key3",
                "cli-input-value3",
            ],
        )
        self.assertTrue(result.exit_code == 0)
        self.assertEqual(self.tq.queue.qsize(), 1)

        result = self.tq.dequeue()
        self.assertDictEqual(
            result,
            {
                "cli-input-key1": "cli-input-value1",
                "cli-input-key2": "cli-input-value2",
                "cli-input-key3": "cli-input-value3",
            },
        )

    def test_cli_integration_dequeues_task(self):
        """Test that CLI dequeues task from CLI"""

        task = {"cli-input-key": "cli-input-value"}
        self.assertEqual(self.tq.queue.qsize(), 0)
        self.tq.enqueue(task)
        self.assertEqual(self.tq.queue.qsize(), 1)

        runner = CliRunner()
        result = runner.invoke(
            cli.dequeue,
            ["--broker-url", self.broker_url, "--queue-name", self.queue_name],
        )
        self.assertTrue(result.exit_code == 0)
        result = json.loads(result.output)
        self.assertDictEqual(result, task)
