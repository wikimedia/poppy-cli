==========
poppy-cli
==========

.. image:: https://github.com/johngian/poppy-cli/actions/workflows/tests.yml/badge.svg?branch=master
        :target: https://github.com/johngian/poppy-cli/actions/workflows/tests.yml


Poppy is a simple message queue CLI tool

* Free software: GNU General Public License v3

Features
--------

* Simple CLI implementation to enqueue/dequeue messages
* Supports a variety of broker backends
* Designed to act as a glue for CLI utils chaining to enqueue/dequeue tasks without any development effort

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.
All the heavylifting for messaging is handle by Kombu_.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _Kombu: https://github.com/celery/kombu
