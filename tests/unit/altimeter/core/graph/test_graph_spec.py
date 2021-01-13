from typing import Any, List, Type
from unittest import TestCase

from altimeter.core.graph.graph_spec import GraphSpec
from altimeter.core.graph.links import LinkCollection
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec


class TestResourceSpecA(ResourceSpec):
    type_name = "a"

    @classmethod
    def get_full_type_name(self):
        return "test:a"

    @classmethod
    def scan(cls: Type["TestResourceSpecA"], scan_accessor: Any) -> List[Resource]:
        resources = [
            Resource(resource_id="123", type=cls.type_name, link_collection=LinkCollection()),
            Resource(resource_id="456", type=cls.type_name, link_collection=LinkCollection()),
        ]
        return resources


class TestResourceSpecB(ResourceSpec):
    type_name = "b"

    @classmethod
    def get_full_type_name(self):
        return "test:b"

    @classmethod
    def scan(cls: Type["TestResourceSpecB"], scan_accessor: Any) -> List[Resource]:
        resources = [
            Resource(resource_id="abc", type=cls.type_name, link_collection=LinkCollection()),
            Resource(resource_id="def", type=cls.type_name, link_collection=LinkCollection()),
        ]
        return resources


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
        resources = graph_spec.scan()

        expected_resources = [
            Resource(resource_id="123", type="a", link_collection=LinkCollection()),
            Resource(resource_id="456", type="a", link_collection=LinkCollection()),
            Resource(resource_id="abc", type="b", link_collection=LinkCollection()),
            Resource(resource_id="def", type="b", link_collection=LinkCollection()),
        ]
        self.assertEqual(resources, expected_resources)
