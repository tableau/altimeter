import json
from typing import Any, List
from unittest import TestCase

from altimeter.core.graph.field.exceptions import (
    ResourceLinkFieldSourceKeyNotFoundException,
    ResourceLinkFieldValueNotAStringException,
)
from altimeter.core.graph.schema import Schema
from altimeter.core.graph.links import LinkCollection, ResourceLink
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec

from altimeter.core.graph.field.resource_link_field import (
    EmbeddedResourceLinkField,
    ResourceLinkField,
)


class TestResourceSpec(ResourceSpec):
    type_name = "test_type_name"
    schema = Schema()

    @classmethod
    def scan(cls, scan_accessor: Any) -> List[Resource]:
        return []

    @classmethod
    def get_full_type_name(cls) -> str:
        return cls.type_name


class TestResourceLinkField(TestCase):
    def test_valid_input(self):
        input_str = '{"FieldName": "Value"}'
        field = ResourceLinkField("FieldName", TestResourceSpec)

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="test_type_name", obj="test_type_name:Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_valid_input_str_classname(self):
        input_str = '{"FieldName": "Value"}'
        field = ResourceLinkField("FieldName", "TestResourceSpec")

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="test_type_name", obj="test_type_name:Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_valid_input_with_alti_key(self):
        input_str = '{"FieldName": "Value"}'
        field = ResourceLinkField("FieldName", TestResourceSpec, alti_key="alti_field_name")

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="alti_field_name", obj="test_type_name:Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_key_present_with_optional(self):
        input_str = '{"FieldName": "Value"}'
        field = ResourceLinkField("FieldName", TestResourceSpec, optional=True)

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="test_type_name", obj="test_type_name:Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_key_absent_with_optional(self):
        input_str = "{}"
        field = ResourceLinkField("FieldName", TestResourceSpec, optional=True)
        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        self.assertEqual(link_collection, LinkCollection())

    def test_key_absent_without_optional(self):
        input_str = "{}"
        field = ResourceLinkField("FieldName", TestResourceSpec)

        input_data = json.loads(input_str)
        with self.assertRaises(ResourceLinkFieldSourceKeyNotFoundException):
            field.parse(data=input_data, context={})

    def test_value_not_a_string(self):
        input_str = '{"FieldName": [1, 2, 3]}'
        field = ResourceLinkField("FieldName", TestResourceSpec)

        input_data = json.loads(input_str)
        with self.assertRaises(ResourceLinkFieldValueNotAStringException):
            field.parse(data=input_data, context={})

    def test_value_is_id(self):
        input_str = '{"FieldName": "Value"}'
        field = ResourceLinkField("FieldName", TestResourceSpec, value_is_id=True)
        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="test_type_name", obj="Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)


class TestEmbeddedResourceLinkField(TestCase):
    def test_valid_input(self):
        input_data = "foo"
        field = EmbeddedResourceLinkField(TestResourceSpec)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="test_type_name", obj="test_type_name:foo"),)
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_valid_input_str_classname(self):
        input_data = "foo"
        field = EmbeddedResourceLinkField("TestResourceSpec")
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="test_type_name", obj="test_type_name:foo"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_valid_input_value_is_id(self):
        input_data = "foo"
        field = EmbeddedResourceLinkField("TestResourceSpec", value_is_id=True)

        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            resource_links=(ResourceLink(pred="test_type_name", obj="foo"),),
        )
        self.assertEqual(link_collection, expected_link_collection)
