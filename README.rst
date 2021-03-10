==========
poppy-cli
==========


.. image:: https://img.shields.io/pypi/v/poppy-cli.svg
        :target: https://pypi.python.org/pypi/poppy-cli

.. image:: https://img.shields.io/travis/johngian/poppy-cli.svg
        :target: https://travis-ci.com/johngian/poppy-cli

.. image:: https://readthedocs.org/projects/poppy-cli/badge/?version=latest
        :target: https://poppy-cli.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status




Poppy is a simple message queue CLI tool


* Free software: GNU General Public License v3
* Documentation: https://poppy-cli.readthedocs.io.


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
