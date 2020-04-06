Developer's Guide
=================

This document contains information on setting up a development environment.

Requirements
------------

Altimeter requires Python 3.7 or greater.

To install project requirements, from the base repo dir:

::

    find . -name requirements.txt -exec pip install -r {} \;

Pre-Commit Check
----------------

A pre-commit script is included (`git/pre-commit.sh`) which performs static analysis
using mypy_ and pylint_, code autoformat checking using black_ and runs tests.

This script is run as a part of Altimeter's CI and must pass for contributions
to be merged.

To configure this as a pre-commit hook, from the base repository directory:

::

    ln -s ../../git/pre-commit.sh .git/hooks/pre-commit

Running these parts in isolation is described below.

Static Analysis and Code Formatting
-----------------------------------

To run mypy_, pylint_ and black_ checks:

::

    ci/static_checks.sh altimeter

Note that mypy_ is ran with the `--disallow-untyped-defs` option which
actively requires type definitions in functions.

Running Tests
-------------

Run tests using the provided helper script:

::

    ci/tests.sh

*Next Steps*

See :doc:`Extending Altimeter <extending>` for a guide to extending Altimeter's
capabilities to collect and graph more data.

.. _black: https://github.com/psf/black
.. _mypy: https://github.com/python/mypy
.. _pylint: https://github.com/PyCQA/pylint
