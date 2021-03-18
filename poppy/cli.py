"""Console script for poppy."""
from contextlib import closing
from typing import Dict, Tuple

import click

from poppy.engine import ConfigDict
from poppy.messsaging import Queue

DEFAULT_CONFIG: ConfigDict = Queue.get_default_config()


@click.group()
@click.option("--broker-url", help="Message queue broker URL", required=True)
@click.option("--queue-name", help="Message queue name", required=True)
@click.option(
    "--connection-timeout",
    type=int,
    help="Connection timeout (s)",
    default=DEFAULT_CONFIG["CONNECTION_TIMEOUT"],
    show_default=True,
)
@click.pass_context
def main(
    ctx: click.core.Context, broker_url: str, queue_name: str, connection_timeout: int
):
    """Simple CLI for messaging"""

    ctx.ensure_object(dict)
    ctx.obj["BROKER_URL"] = broker_url
    ctx.obj["QUEUE_NAME"] = queue_name
    ctx.obj["CONNECTION_TIMEOUT"] = connection_timeout


@main.command()
@click.option(
    "--message-meta",
    type=(str, str),
    help="Message metadata key/value pair",
    multiple=True,
    required=True,
)
@click.pass_context
def enqueue(ctx: click.core.Context, message_meta: Tuple[Tuple[str, str]]):
    """Enqueue a message to the queue"""

    # Convert input key/value to message
    message: Dict[str, str] = {k: v for (k, v) in message_meta}
    with closing(Queue(ctx.obj)) as queue:
        queue.enqueue(message)


@main.command()
@click.option(
    "--batch",
    type=int,
    help="Number of messages to be dequeued",
    default=1,
    show_default=True,
)
@click.option(
    "--blocking-dequeue",
    type=bool,
    help="Blocking dequeue operation",
    default=DEFAULT_CONFIG["BLOCKING_DEQUEUE"],
    show_default=True,
)
@click.option(
    "--blocking-dequeue-timeout",
    type=int,
    help="Dequeue block timeout",
    default=DEFAULT_CONFIG["DEQUEUE_TIMEOUT"],
    show_default=True,
)
@click.option(
    "--dequeue-raise-on-empty",
    type=bool,
    help="Raise error on empty queue",
    default=DEFAULT_CONFIG["RAISE_ON_EMPTY_DEQUEUE"],
    show_default=True,
)
@click.option(
    "--consumer-group-id", type=str, help="Kafka consumer group ID", required=False
)
@click.option(
    "--consumer-autocommit",
    type=bool,
    help="Kafka consumer autocommit",
    default=DEFAULT_CONFIG["CONSUMER_AUTOCOMMIT"],
    show_default=True,
)
@click.option(
    "--consumer-auto-offset-reset",
    type=str,
    help="Kafka consumer auto offset reset",
    default=DEFAULT_CONFIG["CONSUMER_AUTO_OFFSET_RESET"],
    show_default=True,
)
@click.pass_context
def dequeue(
    ctx: click.core.Context,
    batch: int,
    blocking_dequeue: bool,
    blocking_dequeue_timeout: int,
    dequeue_raise_on_empty: bool,
    consumer_group_id: str,
    consumer_autocommit: bool,
    consumer_auto_offset_reset: str,
):
    """Dequeue message from the queue"""

    ctx.obj["BLOCKING_DEQUEUE"] = blocking_dequeue
    ctx.obj["DEQUEUE_TIMEOUT"] = blocking_dequeue_timeout
    ctx.obj["RAISE_ON_EMPTY_DEQUEUE"] = dequeue_raise_on_empty
    ctx.obj["CONSUMER_AUTOCOMMIT"] = consumer_autocommit
    ctx.obj["CONSUMER_AUTO_OFFSET_RESET"] = consumer_auto_offset_reset

    if consumer_group_id:
        ctx.obj["CONSUMER_GROUP_ID"] = consumer_group_id

    with closing(Queue(ctx.obj)) as queue:
        for _ in range(batch):
            message = queue.dequeue()
            click.echo(message)
