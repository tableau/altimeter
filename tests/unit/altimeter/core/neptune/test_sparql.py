from unittest import TestCase

from altimeter.core.neptune.sparql import finalize_query, InvalidQueryException


class TestFinalizeQuerySingleGraph(TestCase):
    def test_empty_query(self):
        query = ""
        graph_uris = ["http://graph/1"]

        with self.assertRaises(InvalidQueryException):
            finalize_query(query=query, graph_uris=graph_uris)

    def test_one_line_query(self):
        query = "select ?s ?p ?o where {?s ?p ?o}"
        graph_uris = ["http://graph/1"]

        expected_finalized_query = "select ?s ?p ?o FROM <http://graph/1> where {?s ?p ?o}"
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)

    def test_one_line_query_with_trailing_comment(self):
        query = "select ?s ?p ?o where {?s ?p ?o} # hi"
        graph_uris = ["http://graph/1"]

        expected_finalized_query = "select ?s ?p ?o FROM <http://graph/1> where {?s ?p ?o} # hi"
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)

    def test_one_line_query_with_limit(self):
        query = "select ?s ?p ?o where {?s ?p ?o} limit 100"
        graph_uris = ["http://graph/1"]

        expected_finalized_query = (
            "select ?s ?p ?o FROM <http://graph/1> where {?s ?p ?o} limit 100"
        )
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)

    def test_one_line_query_with_trailing(self):
        query = "select ?s ?p ?o where {?s ?p ?o} # ignore me please select where {"
        graph_uris = ["http://graph/1"]

        expected_finalized_query = "select ?s ?p ?o FROM <http://graph/1> where {?s ?p ?o} # ignore me please select where {"
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)

    def test_one_line_query_missing_where(self):
        query = "select ?s ?p ?o {?s ?p ?o}"
        graph_uris = ["http://graph/1"]

        with self.assertRaises(InvalidQueryException):
            finalize_query(query=query, graph_uris=graph_uris)

    def test_one_line_query_missing_where_due_to_comment(self):
        query = "select ?s ?p ?o # where {?s ?p ?o}"
        graph_uris = ["http://graph/1"]

        with self.assertRaises(InvalidQueryException):
            finalize_query(query=query, graph_uris=graph_uris)

    def test_one_line_query_missing_where_due_to_leading_comment(self):
        query = "#select ?s ?p ?o where {?s ?p ?o}"
        graph_uris = ["http://graph/1"]

        with self.assertRaises(InvalidQueryException):
            finalize_query(query=query, graph_uris=graph_uris)

    def test_one_line_query_full_rdf_type_syntax(self):
        query = "select ?s ?p ?o where { ?s ?p ?o; <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <my:type> }"
        graph_uris = ["http://graph/1"]

        expected_finalized_query = "select ?s ?p ?o FROM <http://graph/1> where { ?s ?p ?o; <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <my:type> }"
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)

    def test_multi_line_query(self):
        query = "select ?s ?p ?o\nwhere {?s ?p ?o}"
        graph_uris = ["http://graph/1"]

        expected_finalized_query = "select ?s ?p ?o\nFROM <http://graph/1> where {?s ?p ?o}"
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)

    def test_multi_line_query_with_trailing_comments(self):
        query = "select ?s ?p ?o # this is the select\nwhere {?s ?p ?o} # this is the where clause"
        graph_uris = ["http://graph/1"]

        expected_finalized_query = "select ?s ?p ?o # this is the select\nFROM <http://graph/1> where {?s ?p ?o} # this is the where clause"
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)

    def test_multi_line_query_missing_where(self):
        query = "select ?s ?p ?o\n{?s ?p ?o}"
        graph_uris = ["http://graph/1"]

        with self.assertRaises(InvalidQueryException):
            finalize_query(query=query, graph_uris=graph_uris)

    def test_multi_line_query_with_comments(self):
        query = "# select ?s ?p ?o where { ?s ?p ?o }\n# that was an old version\nselect ?s ?p ?o  # where { not this where\n# or this where {\nwhere  # this where\n{ ?s  # sub\n?p  # pred\n?o\nobj\n}  # bye\n# test\n# select ?s ?p ?o where { ?s ?p ?o } # limit 100\n limit 1000 #real limit"
        graph_uris = ["http://graph/1", "http://graph/2"]

        expected_finalized_query = "# select ?s ?p ?o where { ?s ?p ?o }\n# that was an old version\nselect ?s ?p ?o  # where { not this where\n# or this where {\nFROM <http://graph/1> FROM <http://graph/2> where  # this where\n{ ?s  # sub\n?p  # pred\n?o\nobj\n}  # bye\n# test\n# select ?s ?p ?o where { ?s ?p ?o } # limit 100\n limit 1000 #real limit"
        finalized_query = finalize_query(query=query, graph_uris=graph_uris)

        self.assertEqual(finalized_query, expected_finalized_query)
