Altimeter
=========

Altimeter is a system to scan AWS resources across multiple AWS
Organizations and Accounts and link resources together in a graph
which can be queried to discover security and compliance issues.

Altimeter outputs RDF files which can be loaded into a triplestore
such as AWS Neptune and Blazegraph.

This documentation is divided into 3 parts:

`User Documentation`_ - How to use Altimeter to scan your AWS accounts and query relationships.

`Developer Documentation`_ - Documentation for developers who are interested in extending Altimeter's capabilities,
specifically graphing additional AWS resource types or additonal fields on existing resource types.

`API Documentation`_ - auto-generated API documentation.

User Documentation
------------------

.. toctree::
    :hidden:
    :caption: User Documentation

    user/quickstart
    user/multiaccount_scanning
    user/local_blazegraph
    user/sample_queries

:doc:`Quickstart <user/quickstart>` - Introductory guide to graphing a single AWS account.


:doc:`MultiAccount Scanning <user/multiaccount_scanning>` - How to graph multiple accounts and organizations.

Developer Documentation
-----------------------

.. toctree::
    :hidden:
    :caption: Developer Documentation

    dev/devguide
    dev/extending

:doc:`Developer's Guide <dev/devguide>` - Start here.

:doc:`Extending Altimeter <dev/extending>` - How to extend Altimeter's capabilities to collect and graph more data.

API Documentation
-----------------

.. toctree::
    :hidden:
    :caption: API Documentation

    modules
    altimeter.aws
    altimeter.core

Altimeter is divided into two packages - `aws` and `core`.

The :doc:`aws <altimeter.aws>` package contains AWS-specific access and scanning code for AWS-specific
resources.

The :doc:`core <altimeter.core>` package contains generic graphing code which is used by the `aws` package but could be
used by other graph-generating systems.


