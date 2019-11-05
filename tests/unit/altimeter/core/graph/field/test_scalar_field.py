import json
from unittest import TestCase

from altimeter.core.graph.field.exceptions import (
    ScalarFieldSourceKeyNotFoundException,
    ScalarFieldValueNotAScalarException,
    ParentKeyMissingException,
)
from altimeter.core.graph.field.scalar_field import EmbeddedScalarField, ScalarField


class TestScalarField(TestCase):
    def test_valid_input(self):
        input_str = '{"FieldName": "Value"}'
        field = ScalarField("FieldName")
        expected_output_data = [{"pred": "field_name", "obj": "Value", "type": "simple"}]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_valid_input_with_alti_key(self):
        input_str = '{"FieldName": "Value"}'
        field = ScalarField("FieldName", alti_key="alti_field_name")
        expected_output_data = [{"pred": "alti_field_name", "obj": "Value", "type": "simple"}]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_key_present_with_optional(self):
        input_str = '{"FieldName": "Value"}'
        field = ScalarField("FieldName", optional=True)
        expected_output_data = [{"pred": "field_name", "obj": "Value", "type": "simple"}]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_key_absent_with_optional(self):
        input_str = "{}"
        field = ScalarField("FieldName", optional=True)
        expected_output_data = []

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_key_absent_without_optional(self):
        input_str = "{}"
        field = ScalarField("FieldName")

        input_data = json.loads(input_str)
        with self.assertRaises(ScalarFieldSourceKeyNotFoundException):
            field.parse(data=input_data, context={})

    def test_key_absent_with_default(self):
        input_str = "{}"
        field = ScalarField("FieldName", default_value="DefaultValue")
        expected_output_data = [{"pred": "field_name", "obj": "DefaultValue", "type": "simple"}]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

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
        expected_output_data = [{"pred": "parent_alti_key", "obj": "foo", "type": "simple"}]

        links = field.parse(data=input_data, context={"parent_alti_key": parent_alti_key})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

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
