==========
poppy-cli
==========

.. image:: https://github.com/johngian/poppy-cli/actions/workflows/tests.yml/badge.svg?branch=master
        :target: https://github.com/johngian/poppy-cli/actions/workflows/tests.yml

.. image:: https://github.com/johngian/poppy-cli/actions/workflows/docs.yml/badge.svg?branch=master
        :target: https://johngian.github.io/poppy-cli/

.. image:: https://sonarcloud.io/api/project_badges/measure?project=johngian_poppy-cli&metric=alert_status
        :target: https://sonarcloud.io/dashboard?id=johngian_poppy-cli

.. image:: https://codecov.io/gh/johngian/poppy-cli/branch/master/graph/badge.svg?token=OOTW29VBKV
        :target: https://codecov.io/gh/johngian/poppy-cli


Poppy is a simple message queue CLI tool

Features
--------

* Simple CLI implementation to enqueue/dequeue messages
* Supports a variety of broker backends
* Designed to act as a glue for CLI utils chaining to enqueue/dequeue messages without any development effort
* Extensive unit/integration testing and static type checking using mypy
* Allows both single and batched message dequeuing

Supported backends
------------------

* Using `python-kafka`
   * Kafka
* Using `kombu`
   * File based
   * SQL backend
      * Engines supported by SQLAlchemy
   * Redis
   * MongoDB
   * AMQP
   * QPID
   * Cloud services
      * AWS
         * SQS
      * Azure
         * Service Bus
         * Storage Queues
   * Zookeeper
   * Consul
   * Etcd

List doesn't include kombu backends that are python specific or in memory because they are out of project scope.

License
-------

GNU General Public License v3

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.
All the heavylifting for messaging is handle by kombu_ and `kafka-python <https://github.com/dpkp/kafka-python>`_.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _Kombu: https://github.com/celery/kombu
