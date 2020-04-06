from typing import Any, List, Type
from unittest import TestCase

from altimeter.core.graph.graph_spec import GraphSpec
from altimeter.core.multilevel_counter import MultilevelCounter
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
            Resource(resource_id="123", type_name=cls.type_name, links=[]),
            Resource(resource_id="456", type_name=cls.type_name, links=[]),
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
            Resource(resource_id="abc", type_name=cls.type_name, links=[]),
            Resource(resource_id="def", type_name=cls.type_name, links=[]),
        ]
        return resources


class TestScanAccessor:
    api_call_stats: MultilevelCounter = MultilevelCounter()


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
        expected_resource_dicts = [
            Resource(resource_id="123", type_name=TestResourceSpecA.type_name).to_dict(),
            Resource(resource_id="456", type_name=TestResourceSpecA.type_name).to_dict(),
            Resource(resource_id="abc", type_name=TestResourceSpecB.type_name).to_dict(),
            Resource(resource_id="def", type_name=TestResourceSpecB.type_name).to_dict(),
        ]
        self.assertCountEqual(
            [resource.to_dict() for resource in resources], expected_resource_dicts
        )
