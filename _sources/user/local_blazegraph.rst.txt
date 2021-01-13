Local Querying with Blazegraph
==============================

Once you've generated an RDF (:doc:`Quickstart <quickstart>`) you
can load these results into a local Blazegraph instance for querying:

::

    rdf2blaze <path-to-rdf>

This command will start a Blazegraph docker container, load the rdf and print
details on accessing it:

::

    Query UI is available at http://localhost:8889/bigdata/#query

    Hit CTRL-C to exit.

Loading the url above in the browser will open a UI where SPARQL queries can
be run against the graph.

Some sample SPARQL queries are included in :doc:`Sample Queries <sample_queries>`.
