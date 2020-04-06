from typing import Any, List, Type
from unittest import TestCase

from altimeter.core.graph.exceptions import (
    GraphSetOrphanedReferencesException,
    UnmergableDuplicateResourceIdsFoundException,
    UnmergableGraphSetsException,
)
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.link.links import ResourceLinkLink, SimpleLink
from altimeter.core.multilevel_counter import MultilevelCounter
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
            resource_id="123", type_name="test:a", links=[SimpleLink(pred="has-foo", obj="goo")]
        )
        resource_a2 = Resource(resource_id="456", type_name="test:a")
        resource_b1 = Resource(
            resource_id="abc", type_name="test:b", links=[ResourceLinkLink(pred="has-a", obj="123")]
        )
        resource_b2 = Resource(
            resource_id="def", type_name="test:b", links=[SimpleLink(pred="name", obj="sue")]
        )
        resources = [resource_a1, resource_a2, resource_b1, resource_b2]
        self.graph_set = GraphSet(
            name="test-name",
            version="1",
            start_time=1234,
            end_time=4567,
            resources=resources,
            errors=["test err 1", "test err 2"],
            stats=MultilevelCounter(),
        )

    def test_rdf_a_type(self):
        graph = self.graph_set.to_rdf()

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
        graph = self.graph_set.to_rdf()
        graph.serialize("/tmp/test.rdf")
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
        graph = self.graph_set.to_rdf()

        err_results = graph.query("select ?o where { ?s <test-name:error> ?o } order by ?o")
        err_strs = []
        expected_err_strs = ["test err 1", "test err 2"]
        for err_result in err_results:
            self.assertEqual(1, len(err_result))
            err_strs.append(str(err_result[0]))
        self.assertEqual(err_strs, expected_err_strs)

    def test_to_dict(self):
        expected_dict = {
            "name": "test-name",
            "version": "1",
            "start_time": 1234,
            "end_time": 4567,
            "resources": {
                "123": {
                    "type": "test:a",
                    "links": [{"pred": "has-foo", "obj": "goo", "type": "simple"}],
                },
                "456": {"type": "test:a"},
                "abc": {
                    "type": "test:b",
                    "links": [{"pred": "has-a", "obj": "123", "type": "resource_link"}],
                },
                "def": {
                    "type": "test:b",
                    "links": [{"pred": "name", "obj": "sue", "type": "simple"}],
                },
            },
            "errors": ["test err 1", "test err 2"],
            "stats": {"count": 0},
        }
        self.assertDictEqual(expected_dict, self.graph_set.to_dict())

    def test_from_dict(self):
        input_dict = {
            "name": "test-name",
            "version": "1",
            "start_time": 1234,
            "end_time": 4567,
            "resources": {
                "123": {
                    "type": "test:a",
                    "links": [{"pred": "has-foo", "obj": "goo", "type": "simple"}],
                },
                "456": {"type": "test:a"},
                "abc": {
                    "type": "test:b",
                    "links": [{"pred": "has-a", "obj": "123", "type": "resource_link"}],
                },
                "def": {
                    "type": "test:b",
                    "links": [{"pred": "name", "obj": "sue", "type": "simple"}],
                },
            },
            "errors": ["test err 1", "test err 2"],
            "stats": {"count": 0},
        }
        graph_set = GraphSet.from_dict(input_dict)
        self.assertEqual(graph_set.to_dict(), input_dict)

    def test_validate(self):
        self.graph_set.validate()


class TestGraphSetWithValidDataMerging(TestCase):
    def setUp(self):
        resource_a1 = Resource(
            resource_id="123", type_name="test:a", links=[SimpleLink(pred="has-foo", obj="goo")]
        )
        resource_a2 = Resource(
            resource_id="123", type_name="test:a", links=[SimpleLink(pred="has-goo", obj="foo")]
        )
        resource_b1 = Resource(
            resource_id="abc", type_name="test:b", links=[ResourceLinkLink(pred="has-a", obj="123")]
        )
        resource_b2 = Resource(
            resource_id="def", type_name="test:b", links=[SimpleLink(pred="name", obj="sue")]
        )
        resources = [resource_a1, resource_a2, resource_b1, resource_b2]
        self.graph_set = GraphSet(
            name="test-name",
            version="1",
            start_time=1234,
            end_time=4567,
            resources=resources,
            errors=["test err 1", "test err 2"],
            stats=MultilevelCounter(),
        )

    def test_rdf_a_type(self):
        graph = self.graph_set.to_rdf()

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

    def test_validate(self):
        self.graph_set.validate()


class TestGraphSetWithInValidData(TestCase):
    def test_unknown_type_name(self):
        resources = [
            Resource(resource_id="xyz", type_name="test:a"),
            Resource(resource_id="xyz", type_name="test:c"),
        ]
        with self.assertRaises(ResourceSpecClassNotFoundException):
            GraphSet(
                name="test-name",
                version="1",
                start_time=1234,
                end_time=4567,
                resources=resources,
                errors=[],
                stats=MultilevelCounter(),
            )

    def test_invalid_resources_dupes_same_class_conflicting_types_no_allow_clobber(self):
        resources = [
            Resource(resource_id="123", type_name="test:a"),
            Resource(resource_id="123", type_name="test:b"),
        ]
        with self.assertRaises(UnmergableDuplicateResourceIdsFoundException):
            GraphSet(
                name="test-name",
                version="1",
                start_time=1234,
                end_time=4567,
                resources=resources,
                errors=[],
                stats=MultilevelCounter(),
            )

    def test_orphaned_ref(self):
        resource_a1 = Resource(
            resource_id="123", type_name="test:a", links=[SimpleLink(pred="has-foo", obj="goo")]
        )
        resource_b1 = Resource(
            resource_id="abc", type_name="test:b", links=[ResourceLinkLink(pred="has-a", obj="456")]
        )
        resources = [resource_a1, resource_b1]
        graph_set = GraphSet(
            name="test-name",
            version="1",
            start_time=1234,
            end_time=4567,
            resources=resources,
            errors=["test err 1", "test err 2"],
            stats=MultilevelCounter(),
        )
        with self.assertRaises(GraphSetOrphanedReferencesException):
            graph_set.validate()


class TestGraphSetMerge(TestCase):
    def test_invalid_diff_names(self):
        graph_set_1 = GraphSet(
            name="graph-1",
            version="1",
            start_time=10,
            end_time=20,
            resources=[],
            errors=[],
            stats=MultilevelCounter(),
        )
        graph_set_2 = GraphSet(
            name="graph-2",
            version="1",
            start_time=15,
            end_time=25,
            resources=[],
            errors=[],
            stats=MultilevelCounter(),
        )
        with self.assertRaises(UnmergableGraphSetsException):
            graph_set_1.merge(graph_set_2)

    def test_invalid_diff_versions(self):
        graph_set_1 = GraphSet(
            name="graph-1",
            version="1",
            start_time=10,
            end_time=20,
            resources=[],
            errors=[],
            stats=MultilevelCounter(),
        )
        graph_set_2 = GraphSet(
            name="graph-1",
            version="2",
            start_time=15,
            end_time=25,
            resources=[],
            errors=[],
            stats=MultilevelCounter(),
        )
        with self.assertRaises(UnmergableGraphSetsException):
            graph_set_1.merge(graph_set_2)

    def test_valid_merge(self):
        resource_a1 = Resource(
            resource_id="123", type_name="test:a", links=[SimpleLink(pred="has-foo", obj="goo")]
        )
        resource_a2 = Resource(resource_id="456", type_name="test:a")
        resource_b1 = Resource(
            resource_id="abc", type_name="test:b", links=[ResourceLinkLink(pred="has-a", obj="123")]
        )
        resource_b2 = Resource(
            resource_id="def", type_name="test:b", links=[SimpleLink(pred="name", obj="sue")]
        )
        graph_set_1 = GraphSet(
            name="graph-1",
            version="1",
            start_time=10,
            end_time=20,
            resources=[resource_a1, resource_a2],
            errors=["errora1", "errora2"],
            stats=MultilevelCounter(),
        )
        graph_set_2 = GraphSet(
            name="graph-1",
            version="1",
            start_time=15,
            end_time=25,
            resources=[resource_b1, resource_b2],
            errors=["errorb1", "errorb2"],
            stats=MultilevelCounter(),
        )
        graph_set_1.merge(graph_set_2)

        self.assertEqual(graph_set_1.name, "graph-1")
        self.assertEqual(graph_set_1.version, "1")
        self.assertEqual(graph_set_1.start_time, 10)
        self.assertEqual(graph_set_1.end_time, 25)
        self.assertCountEqual(graph_set_1.errors, ["errora1", "errora2", "errorb1", "errorb2"])
        expected_resource_dicts = [
            {"type": "test:a", "links": [{"pred": "has-foo", "obj": "goo", "type": "simple"}]},
            {"type": "test:a"},
            {"type": "test:b", "links": [{"pred": "has-a", "obj": "123", "type": "resource_link"}]},
            {"type": "test:b", "links": [{"pred": "name", "obj": "sue", "type": "simple"}]},
        ]
        resource_dicts = [resource.to_dict() for resource in graph_set_1.resources]
        self.assertCountEqual(expected_resource_dicts, resource_dicts)
