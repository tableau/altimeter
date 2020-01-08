Quickstart
==========

This quickstart guide demonstrates how to generate a graph for a single account.

Installation
------------

::

    pip install altimeter

Generating the Graph
--------------------

Assuming you have configured AWS CLI credentials
(see https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html ), run:

::

    altimeter --base_dir /tmp/altimeter --regions us-east-1

This will scan all resources in the *us-east-1* region.  Multiple regions can be specified
in this argument or it can be omitted entirely in which case all regions will be scanned.

The full path to the generated RDF file will printed, for example:

    Created /tmp/altimeter/20191018/1571425383/graph.rdf

This RDF file can then be loaded into a triplestore such as Neptune or Blazegraph for querying.

Tooling is included for loading into a local Blazegraph instance, see
:doc:`Local Querying with Blazegraph <local_blazegraph>`

*Next Steps*

The ability to graph a single account is useful, but Altimeter really shines
in cases where resources are spread across multiple accounts.  See
:doc:`MultiAccount Scanning <multiaccount_scanning>` to learn how to graph
multiple accounts.
