import json
from unittest import TestCase

from altimeter.core.graph.field.exceptions import (
    ScalarFieldSourceKeyNotFoundException,
    ScalarFieldValueNotAScalarException,
    ParentKeyMissingException,
)
from altimeter.core.graph.field.scalar_field import EmbeddedScalarField, ScalarField
from altimeter.core.graph.links import LinkCollection, SimpleLink


class TestScalarField(TestCase):
    def test_valid_input(self):
        input_str = '{"FieldName": "Value"}'
        field = ScalarField("FieldName")

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            simple_links=(SimpleLink(pred="field_name", obj="Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_valid_input_with_alti_key(self):
        input_str = '{"FieldName": "Value"}'
        field = ScalarField("FieldName", alti_key="alti_field_name")

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            simple_links=(SimpleLink(pred="alti_field_name", obj="Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_key_present_with_optional(self):
        input_str = '{"FieldName": "Value"}'
        field = ScalarField("FieldName", optional=True)

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            simple_links=(SimpleLink(pred="field_name", obj="Value"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_key_absent_with_optional(self):
        input_str = "{}"
        field = ScalarField("FieldName", optional=True)

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        self.assertEqual(link_collection, LinkCollection())

    def test_key_absent_without_optional(self):
        input_str = "{}"
        field = ScalarField("FieldName")

        input_data = json.loads(input_str)
        with self.assertRaises(ScalarFieldSourceKeyNotFoundException):
            field.parse(data=input_data, context={})

    def test_key_absent_with_default(self):
        input_str = "{}"
        field = ScalarField("FieldName", default_value="DefaultValue")

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            simple_links=(SimpleLink(pred="field_name", obj="DefaultValue"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_value_not_scalar(self):
        input_str = '{"FieldName": [1, 2, 3]}'
        field = ScalarField("FieldName")

        input_data = json.loads(input_str)
        with self.assertRaises(ScalarFieldValueNotAScalarException):
            field.parse(data=input_data, context={})


class TestEmbeddedScalarField(TestCase):
    def test_valid_input(self):
        input_data = "foo"
        parent_alti_key = "parent_alti_key"
        field = EmbeddedScalarField()

        link_collection = field.parse(data=input_data, context={"parent_alti_key": parent_alti_key})

        expected_link_collection = LinkCollection(
            simple_links=(SimpleLink(pred="parent_alti_key", obj="foo"),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_value_not_scalar(self):
        input_str = "[1, 2, 3]"
        parent_alti_key = "parent_alti_key"
        field = EmbeddedScalarField()

        input_data = json.loads(input_str)
        with self.assertRaises(ScalarFieldValueNotAScalarException):
            field.parse(data=input_data, context={"parent_alti_key": parent_alti_key})

    def test_missing_parent_alti_key(self):
        input_data = "foo"
        field = EmbeddedScalarField()

        with self.assertRaises(ParentKeyMissingException):
            field.parse(data=input_data, context={})

    def test_missing_parent_alti_key_value(self):
        input_data = "foo"
        field = EmbeddedScalarField()

        with self.assertRaises(ParentKeyMissingException):
            field.parse(data=input_data, context={"parent_alti_key": None})
