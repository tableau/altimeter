from typing import Any, List, Type
from unittest import TestCase

from altimeter.core.graph.exceptions import (
    GraphSetOrphanedReferencesException,
    UnmergableDuplicateResourceIdsFoundException,
    UnmergableGraphSetsException,
)
from altimeter.core.graph.graph_set import GraphSet, ValidatedGraphSet
from altimeter.core.graph.links import LinkCollection, ResourceLink, SimpleLink
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec
from altimeter.core.resource.exceptions import ResourceSpecClassNotFoundException


class TestResourceSpecA(ResourceSpec):
    type_name = "a"

    @classmethod
    def get_full_type_name(self):
        return "test:a"

    @classmethod
    def scan(cls: Type["TestResourceSpecA"], scan_accessor: Any) -> List[Resource]:
        raise NotImplementedError()


class TestResourceSpecB(ResourceSpec):
    type_name = "b"

    @classmethod
    def get_full_type_name(self):
        return "test:b"

    @classmethod
    def scan(cls: Type["TestResourceSpecB"], scan_accessor: Any) -> List[Resource]:
        raise NotImplementedError()


class TestScanAccessor:
    pass


class TestGraphSetWithValidDataNoMerging(TestCase):
    def setUp(self):
        resource_a1 = Resource(
            resource_id="123",
            type="test:a",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="has-foo", obj="goo")]),
        )
        resource_a2 = Resource(resource_id="456", type="test:a", link_collection=LinkCollection(),)
        resource_b1 = Resource(
            resource_id="abc",
            type="test:b",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="has-a", obj="123")]),
        )
        resource_b2 = Resource(
            resource_id="def",
            type="test:b",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="name", obj="sue")]),
        )
        resources = (resource_a1, resource_a2, resource_b1, resource_b2)
        self.validated_graph_set = ValidatedGraphSet(
            name="test-name",
            version="1",
            start_time=1234,
            end_time=4567,
            resources=resources,
            errors=["test err 1", "test err 2"],
        )

    def test_rdf_a_type(self):
        graph = self.validated_graph_set.to_rdf()

        a_results = graph.query(
            "select ?p ?o where {?s a <test-name:test:a> ; ?p ?o} order by ?p ?o"
        )
        expected_a_result_tuples = [
            ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "test-name:test:a"),
            ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "test-name:test:a"),
            ("test-name:has-foo", "goo"),
            ("test-name:id", "123"),
            ("test-name:id", "456"),
        ]
        a_result_tuples = []
        for a_result in a_results:
            self.assertEqual(2, len(a_result))
            a_result_tuples.append((str(a_result[0]), str(a_result[1])))
        self.assertEqual(expected_a_result_tuples, a_result_tuples)

    def test_rdf_b_type(self):
        graph = self.validated_graph_set.to_rdf()
        graph.serialize("/tmp/test.rdf", format="xml")
        linked_a_node_results = graph.query(
            "select ?s where {?s a <test-name:test:a>; <test-name:id> '123' }"
        )
        self.assertEqual(len(linked_a_node_results), 1)
        for linked_a_node_result in linked_a_node_results:
            linked_a_node = str(linked_a_node_result[0])
        b_results = graph.query(
            "select ?p ?o where {?s a <test-name:test:b> ; ?p ?o} order by ?p ?o"
        )
        expected_b_result_tuples = [
            ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "test-name:test:b"),
            ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "test-name:test:b"),
            ("test-name:has-a", str(linked_a_node)),
            ("test-name:id", "abc"),
            ("test-name:id", "def"),
            ("test-name:name", "sue"),
        ]
        b_result_tuples = []
        for b_result in b_results:
            self.assertEqual(2, len(b_result))
            b_result_tuples.append((str(b_result[0]), str(b_result[1])))
        self.assertEqual(expected_b_result_tuples, b_result_tuples)

    def test_rdf_error_graphing(self):
        graph = self.validated_graph_set.to_rdf()

        err_results = graph.query("select ?o where { ?s <test-name:error> ?o } order by ?o")
        err_strs = []
        expected_err_strs = ["test err 1", "test err 2"]
        for err_result in err_results:
            self.assertEqual(1, len(err_result))
            err_strs.append(str(err_result[0]))
        self.assertEqual(err_strs, expected_err_strs)

    def test_graph_content(self):
        expected_resources = (
            Resource(
                resource_id="123",
                type="test:a",
                link_collection=LinkCollection(
                    simple_links=(SimpleLink(pred="has-foo", obj="goo"),),
                ),
            ),
            Resource(resource_id="456", type="test:a", link_collection=LinkCollection(),),
            Resource(
                resource_id="abc",
                type="test:b",
                link_collection=LinkCollection(
                    simple_links=(SimpleLink(pred="has-a", obj="123"),),
                ),
            ),
            Resource(
                resource_id="def",
                type="test:b",
                link_collection=LinkCollection(simple_links=(SimpleLink(pred="name", obj="sue"),),),
            ),
        )
        expected_errors = ["test err 1", "test err 2"]
        self.assertEqual(self.validated_graph_set.resources, expected_resources)
        self.assertEqual(self.validated_graph_set.errors, expected_errors)


class TestGraphSetWithValidDataMerging(TestCase):
    def setUp(self):
        resource_a1 = Resource(
            resource_id="123",
            type="test:a",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="has-foo", obj="goo")]),
        )
        resource_a2 = Resource(
            resource_id="123",
            type="test:a",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="has-goo", obj="foo")]),
        )
        resource_b1 = Resource(
            resource_id="abc",
            type="test:b",
            link_collection=LinkCollection(resource_links=[ResourceLink(pred="has-a", obj="123")]),
        )
        resource_b2 = Resource(
            resource_id="def",
            type="test:b",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="name", obj="sue")]),
        )
        resources = (resource_a1, resource_a2, resource_b1, resource_b2)
        self.validated_graph_set = ValidatedGraphSet(
            name="test-name",
            version="1",
            start_time=1234,
            end_time=4567,
            resources=resources,
            errors=["test err 1", "test err 2"],
        )

    def test_rdf_a_type(self):
        graph = self.validated_graph_set.to_rdf()

        a_results = graph.query(
            "select ?p ?o where {?s a <test-name:test:a> ; ?p ?o} order by ?p ?o"
        )
        expected_a_result_tuples = [
            ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "test-name:test:a"),
            ("test-name:has-foo", "goo"),
            ("test-name:has-goo", "foo"),
            ("test-name:id", "123"),
        ]
        a_result_tuples = []
        for a_result in a_results:
            self.assertEqual(2, len(a_result))
            a_result_tuples.append((str(a_result[0]), str(a_result[1])))
        self.assertEqual(expected_a_result_tuples, a_result_tuples)


class TestGraphSetWithInValidData(TestCase):
    def test_unknown_type_name(self):
        resources = [
            Resource(resource_id="xyz", type="test:a", link_collection=LinkCollection()),
            Resource(resource_id="xyz", type="test:c", link_collection=LinkCollection()),
        ]
        with self.assertRaises(ResourceSpecClassNotFoundException):
            ValidatedGraphSet(
                name="test-name",
                version="1",
                start_time=1234,
                end_time=4567,
                resources=resources,
                errors=[],
            )

    def test_invalid_resources_dupes_same_class_conflicting_types_no_allow_clobber(self):
        resources = [
            Resource(resource_id="123", type="test:a", link_collection=LinkCollection()),
            Resource(resource_id="123", type="test:b", link_collection=LinkCollection()),
        ]
        with self.assertRaises(UnmergableDuplicateResourceIdsFoundException):
            ValidatedGraphSet(
                name="test-name",
                version="1",
                start_time=1234,
                end_time=4567,
                resources=resources,
                errors=[],
            )

    def test_orphaned_ref(self):
        resource_a1 = Resource(
            resource_id="123",
            type="test:a",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="has-foo", obj="goo")]),
        )
        resource_b1 = Resource(
            resource_id="abc",
            type="test:b",
            link_collection=LinkCollection(resource_links=[ResourceLink(pred="has-a", obj="456")]),
        )
        resources = [resource_a1, resource_b1]
        graph_set = GraphSet(
            name="test-name",
            version="1",
            start_time=1234,
            end_time=4567,
            resources=resources,
            errors=["test err 1", "test err 2"],
        )
        with self.assertRaises(GraphSetOrphanedReferencesException):
            ValidatedGraphSet.from_graph_set(graph_set)


class TestGraphSetFromGraphSets(TestCase):
    def test_invalid_diff_names(self):
        graph_set_1 = GraphSet(
            name="graph-1", version="1", start_time=10, end_time=20, resources=[], errors=[],
        )
        graph_set_2 = GraphSet(
            name="graph-2", version="1", start_time=15, end_time=25, resources=[], errors=[],
        )
        with self.assertRaises(UnmergableGraphSetsException):
            GraphSet.from_graph_sets([graph_set_1, graph_set_2])

    def test_invalid_diff_versions(self):
        graph_set_1 = GraphSet(
            name="graph-1", version="1", start_time=10, end_time=20, resources=[], errors=[],
        )
        graph_set_2 = GraphSet(
            name="graph-1", version="2", start_time=15, end_time=25, resources=[], errors=[],
        )
        with self.assertRaises(UnmergableGraphSetsException):
            GraphSet.from_graph_sets([graph_set_1, graph_set_2])

    def test_valid_merge(self):
        resource_a1 = Resource(
            resource_id="123",
            type="test:a",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="has-foo", obj="goo")]),
        )
        resource_a2 = Resource(resource_id="456", type="test:a", link_collection=LinkCollection())
        resource_b1 = Resource(
            resource_id="abc",
            type="test:b",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="has-a", obj="123")]),
        )
        resource_b2 = Resource(
            resource_id="def",
            type="test:b",
            link_collection=LinkCollection(simple_links=[SimpleLink(pred="name", obj="sue")]),
        )
        graph_set_1 = GraphSet(
            name="graph-1",
            version="1",
            start_time=10,
            end_time=20,
            resources=[resource_a1, resource_a2],
            errors=["errora1", "errora2"],
        )
        graph_set_2 = GraphSet(
            name="graph-1",
            version="1",
            start_time=15,
            end_time=25,
            resources=[resource_b1, resource_b2],
            errors=["errorb1", "errorb2"],
        )
        merged_graph_set = ValidatedGraphSet.from_graph_sets([graph_set_1, graph_set_2])

        self.assertEqual(merged_graph_set.name, "graph-1")
        self.assertEqual(merged_graph_set.version, "1")
        self.assertEqual(merged_graph_set.start_time, 10)
        self.assertEqual(merged_graph_set.end_time, 25)
        self.assertCountEqual(merged_graph_set.errors, ["errora1", "errora2", "errorb1", "errorb2"])

        expected_resources = (
            Resource(
                resource_id="123",
                type="test:a",
                link_collection=LinkCollection(
                    simple_links=(SimpleLink(pred="has-foo", obj="goo"),),
                ),
            ),
            Resource(resource_id="456", type="test:a", link_collection=LinkCollection(),),
            Resource(
                resource_id="abc",
                type="test:b",
                link_collection=LinkCollection(
                    simple_links=(SimpleLink(pred="has-a", obj="123"),),
                ),
            ),
            Resource(
                resource_id="def",
                type="test:b",
                link_collection=LinkCollection(simple_links=(SimpleLink(pred="name", obj="sue"),),),
            ),
        )
        expected_errors = ["errora1", "errora2", "errorb1", "errorb2"]

        self.assertCountEqual(merged_graph_set.resources, expected_resources)
        self.assertCountEqual(merged_graph_set.errors, expected_errors)
