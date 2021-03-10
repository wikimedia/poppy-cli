=====
Usage
=====

Top level command
------------------

.. code-block:: bash

    Usage: poppy [OPTIONS] COMMAND [ARGS]...

      Simple CLI for task

    Options:
      --broker-url TEXT             Task queue broker URL  [required]
      --queue-name TEXT             Task queue name  [required]
      --connection-timeout INTEGER  Connection timeout (s)  [default: 5]
      --help                        Show this message and exit.

    Commands:
      dequeue  Dequeue task from the queue
      enqueue  Enqueue a task to the queue


Enqueue command
---------------

.. code-block:: bash

    Usage: poppy enqueue [OPTIONS]

      Enqueue a task to the queue

    Options:
      --task-meta <TEXT TEXT>...  Task metadata key/value pair  [required]
      --help                      Show this message and exit.


Dequeue command
----------------

.. code-block:: bash

    Usage: poppy dequeue [OPTIONS]

      Dequeue task from the queue

    Options:
      --blocking-dequeue BOOLEAN      Blocking dequeue operation  [default: False]
      --blocking-dequeue-timeout INTEGER
                                      Dequeue block timeout  [default: 5]
      --dequeue-raise-on-empty BOOLEAN
                                      Raise error on empty queue  [default: False]
      --help                          Show this message and exit.
