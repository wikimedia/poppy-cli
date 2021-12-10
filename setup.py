#!/usr/bin/env python

"""The setup script."""
import sys
from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = ["Click>=7.0", "kombu>=4.2.1", "kafka-python>=1.4.3"]

if sys.version_info < (3, 8):
    requirements += ["typing-extensions>=3.7.4"]

setup_requirements = []

test_requirements = []

setup(
    author="Yiannis Giannelos",
    author_email="jgiannelos@wikimedia.org",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description="Poppy is a simple message queue CLI tool",
    entry_points={
        "console_scripts": [
            "poppy=poppy.cli:main",
        ],
    },
    install_requires=requirements,
    license="GNU General Public License v3",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="poppy",
    name="poppy",
    packages=find_packages(include=["poppy", "poppy.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    version="0.3.0",
    zip_safe=False,
)
