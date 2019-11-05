from typing import Any, Type, TypeVar
from unittest import TestCase

from altimeter.core.graph.graph_spec import GraphSpec
from altimeter.core.multilevel_counter import MultilevelCounter
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceScanResult, ResourceSpec

T = TypeVar("T", bound="TestResourceSpecA")


class TestResourceSpecA(ResourceSpec):
    type_name = "a"

    @classmethod
    def get_full_type_name(self):
        return "test:a"

    @classmethod
    def scan(cls: Type[T], scan_accessor: Any) -> ResourceScanResult:
        resources = [
            Resource(resource_id="123", type_name=cls.type_name, links=[]),
            Resource(resource_id="456", type_name=cls.type_name, links=[]),
        ]
        return ResourceScanResult(resources=resources, stats=MultilevelCounter(), errors=[])


T = TypeVar("T", bound="TestResourceSpecB")


class TestResourceSpecB(ResourceSpec):
    type_name = "b"

    @classmethod
    def get_full_type_name(self):
        return "test:b"

    @classmethod
    def scan(cls: Type[T], scan_accessor: Any) -> ResourceScanResult:
        resources = [
            Resource(resource_id="abc", type_name=cls.type_name, links=[]),
            Resource(resource_id="def", type_name=cls.type_name, links=[]),
        ]
        return ResourceScanResult(resources=resources, stats=MultilevelCounter(), errors=[])


class TestScanAccessor:
    pass


class TestGraphSpec(TestCase):
    def test_scan(self):
        scan_accessor = TestScanAccessor()
        graph_spec = GraphSpec(
            name="test-name",
            version="1",
            resource_spec_classes=(TestResourceSpecA, TestResourceSpecB),
            scan_accessor=scan_accessor,
        )
        graph_set = graph_spec.scan()
        self.assertEqual(graph_set.name, "test-name")
        self.assertEqual(graph_set.version, "1")
        self.assertEqual(graph_set.errors, [])
        expected_resource_dicts = [
            Resource(resource_id="123", type_name=TestResourceSpecA.type_name).to_dict(),
            Resource(resource_id="456", type_name=TestResourceSpecA.type_name).to_dict(),
            Resource(resource_id="abc", type_name=TestResourceSpecB.type_name).to_dict(),
            Resource(resource_id="def", type_name=TestResourceSpecB.type_name).to_dict(),
        ]
        self.assertCountEqual(
            [resource.to_dict() for resource in graph_set.resources], expected_resource_dicts
        )
