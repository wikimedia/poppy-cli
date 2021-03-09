"""Console script for dashi."""
import json
from contextlib import closing
from typing import Tuple

import click

from dashi.task import TaskQueue


@click.group()
def main():
    """Simple CLI for task"""
    pass


@main.command()
@click.option("--broker-url", help="Task queue broker URL")
@click.option("--queue-name", help="Task queue name")
@click.option(
    "--task-meta", type=(str, str), help="Task metadata key/value pear", multiple=True
)
def enqueue(broker_url: str, queue_name: str, task_meta: Tuple[Tuple[str, str]]):
    """Enqueue a task to the queue"""

    # Convert input key/value to task
    task = {k: v for (k, v) in task_meta}
    with closing(TaskQueue(queue_name, broker_url)) as queue:
        queue.enqueue(task)


@main.command()
@click.option("--broker-url", help="Task queue broker URL")
@click.option("--queue-name", help="Task queue name")
def dequeue(broker_url: str, queue_name: str):
    """Dequeue task from the queue"""

    with closing(TaskQueue(queue_name, broker_url)) as queue:
        task = queue.dequeue()
    click.echo(json.dumps(task))


if __name__ == "__main__":
    main()
