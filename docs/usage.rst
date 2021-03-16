=====
Usage
=====

Top level command
------------------

.. code-block:: bash

    Usage: poppy [OPTIONS] COMMAND [ARGS]...

      Simple CLI for messaging

    Options:
      --broker-url TEXT             Message queue broker URL  [required]
      --queue-name TEXT             Message queue name  [required]
      --connection-timeout INTEGER  Connection timeout (s)  [default: 5]
      --help                        Show this message and exit.

    Commands:
      dequeue  Dequeue message from the queue
      enqueue  Enqueue a message to the queue


Enqueue command
---------------

.. code-block:: bash

    Usage: poppy enqueue [OPTIONS]

      Enqueue a message to the queue

    Options:
      --message-meta <TEXT TEXT>...  Message metadata key/value pair  [required]
      --help                         Show this message and exit.


Dequeue command
----------------

.. code-block:: bash

    Usage: poppy dequeue [OPTIONS]

      Dequeue message from the queue

    Options:
      --blocking-dequeue BOOLEAN      Blocking dequeue operation  [default: False]
      --blocking-dequeue-timeout INTEGER
                                      Dequeue block timeout  [default: 5]
      --dequeue-raise-on-empty BOOLEAN
                                      Raise error on empty queue  [default: False]
      --consumer-group-id TEXT        Kafka consumer group ID
      --consumer-autocommit BOOLEAN   Kafka consumer autocommit  [default: True]
      --consumer-auto-offset-reset TEXT
                                      Kafka consumer auto offset reset  [default:
                                      earliest]
      --help                          Show this message and exit.
