Quickstart
==========

This quickstart guide demonstrates how to generate a graph for a single account.

Prerequisites
-------------

Altimeter requires:

* Python 3.7 or greater
* Docker is strongly recommended for running queries against generated graphs.
* Project dependencies:

::

    pip install -r requirements.txt

* AWS credentials for CLI access to the account you wish to scan.
  See https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

Generating the Graph
--------------------

Run

::

    cd <base-repo-dir>
    bin/altimeter_local.sh --base_dir /tmp/altimeter --regions us-east-1

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
