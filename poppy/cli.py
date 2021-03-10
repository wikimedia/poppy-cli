"""Console script for poppy."""
import json
from contextlib import closing
from typing import Dict, Tuple

import click

from poppy import queue
from poppy.queue import TaskQueue


@click.group()
@click.option("--broker-url", help="Task queue broker URL", required=True)
@click.option("--queue-name", help="Task queue name", required=True)
@click.option(
    "--connection-timeout",
    type=int,
    help="Connection timeout (s)",
    default=queue.DEFAULT_TIMEOUT,
    show_default=True,
)
@click.pass_context
def main(ctx: Dict, broker_url: str, queue_name: str, connection_timeout: int):
    """Simple CLI for task"""

    ctx.obj["BROKER_URL"] = broker_url
    ctx.obj["QUEUE_NAME"] = queue_name
    ctx.obj["CONNECTION_TIMEOUT"] = connection_timeout


@main.command()
@click.option(
    "--task-meta",
    type=(str, str),
    help="Task metadata key/value pair",
    multiple=True,
    required=True,
)
@click.pass_context
def enqueue(ctx: Dict, task_meta: Tuple[Tuple[str, str]]):
    """Enqueue a task to the queue"""

    # Convert input key/value to task
    task = {k: v for (k, v) in task_meta}
    with closing(TaskQueue(ctx.obj)) as queue:
        queue.enqueue(task)


@main.command()
@click.option(
    "--blocking-dequeue",
    type=bool,
    help="Blocking dequeue operation",
    default=queue.DEFAULT_BLOCKING_DEQUEUE,
    show_default=True,
)
@click.option(
    "--blocking-dequeue-timeout",
    type=int,
    help="Dequeue block timeout",
    default=queue.DEFAULT_TIMEOUT,
    show_default=True,
)
@click.option(
    "--dequeue-raise-on-empty",
    type=bool,
    help="Raise error on empty queue",
    default=queue.DEFAULT_RAISE_ON_EMPTY_DEQUEUE,
    show_default=True,
)
@click.pass_context
def dequeue(
    ctx: Dict,
    blocking_dequeue: bool,
    blocking_dequeue_timeout: int,
    dequeue_raise_on_empty: bool,
):
    """Dequeue task from the queue"""

    ctx.obj["BLOCKING_DEQUEUE"] = blocking_dequeue
    ctx.obj["DEQUEUE_TIMEOUT"] = blocking_dequeue_timeout
    ctx.obj["RAISE_ON_EMPTY_DEQUEUE"] = dequeue_raise_on_empty

    with closing(TaskQueue(ctx.obj)) as queue:
        task = queue.dequeue()
    click.echo(json.dumps(task))


if __name__ == "__main__":
    main(obj={})
