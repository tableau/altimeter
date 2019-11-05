import json
from unittest import TestCase

from altimeter.core.graph.exceptions import (
    ListFieldSourceKeyNotFoundException,
    ListFieldValueNotAListException,
)
from altimeter.core.graph.field.dict_field import EmbeddedDictField, DictField
from altimeter.core.graph.field.list_field import AnonymousListField, ListField
from altimeter.core.graph.field.scalar_field import EmbeddedScalarField, ScalarField


class TestListField(TestCase):
    def test_valid_strings_input(self):
        input_str = '{"Animals": ["cow", "pig", "human"]}'
        field = ListField("Animals", EmbeddedScalarField())
        expected_output_data = [
            {"pred": "animals", "obj": "cow", "type": "simple"},
            {"pred": "animals", "obj": "pig", "type": "simple"},
            {"pred": "animals", "obj": "human", "type": "simple"},
        ]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_valid_dicts_input(self):
        input_str = '{"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}'
        field = ListField("People", EmbeddedDictField(ScalarField("Name"), ScalarField("Age")))
        expected_output_data = [
            {
                "pred": "people",
                "obj": [
                    {"pred": "name", "obj": "Bob", "type": "simple"},
                    {"pred": "age", "obj": 49, "type": "simple"},
                ],
                "type": "multi",
            },
            {
                "pred": "people",
                "obj": [
                    {"pred": "name", "obj": "Sue", "type": "simple"},
                    {"pred": "age", "obj": 42, "type": "simple"},
                ],
                "type": "multi",
            },
        ]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_valid_dicts_input_with_alti_key(self):
        input_str = '{"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}'
        field = ListField(
            "People", EmbeddedDictField(ScalarField("Name"), ScalarField("Age")), alti_key="person"
        )
        expected_output_data = [
            {
                "pred": "person",
                "obj": [
                    {"pred": "name", "obj": "Bob", "type": "simple"},
                    {"pred": "age", "obj": 49, "type": "simple"},
                ],
                "type": "multi",
            },
            {
                "pred": "person",
                "obj": [
                    {"pred": "name", "obj": "Sue", "type": "simple"},
                    {"pred": "age", "obj": 42, "type": "simple"},
                ],
                "type": "multi",
            },
        ]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_invalid_input_missing_source_key(self):
        input_str = '{"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}'
        field = ListField(
            "Stuff", EmbeddedDictField(ScalarField("Name"), ScalarField("Age")), alti_key="person"
        )
        input_data = json.loads(input_str)
        with self.assertRaises(ListFieldSourceKeyNotFoundException):
            field.parse(data=input_data, context={"parent_alti_key": "test_parent"})

    def test_invalid_input_not_list(self):
        input_str = '{"People": "foo"}'
        field = ListField(
            "People", EmbeddedDictField(ScalarField("Name"), ScalarField("Age")), alti_key="person"
        )
        input_data = json.loads(input_str)
        with self.assertRaises(ListFieldValueNotAListException):
            field.parse(data=input_data, context={"parent_alti_key": "test_parent"})

    def test_optional(self):
        input_str = "{}"
        field = ListField("People", EmbeddedScalarField(), alti_key="person", optional=True)
        expected_output_data = ()

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_allow_scalar(self):
        input_str = '{"People": "bob"}'
        field = ListField("People", EmbeddedScalarField(), alti_key="person", allow_scalar=True)
        expected_output_data = ({"pred": "person", "obj": "bob", "type": "simple"},)

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)


class TestAnonymousListField(TestCase):
    def test_valid_strings_input(self):
        input_str = '{"Biota": {"Animals": ["cow", "pig", "human"], "Plants": ["tree", "fern"]}}'
        field = DictField("Biota", AnonymousListField("Animals", EmbeddedScalarField()))
        expected_output_data = [
            {
                "pred": "biota",
                "obj": [
                    {"pred": "biota", "obj": "cow", "type": "simple"},
                    {"pred": "biota", "obj": "pig", "type": "simple"},
                    {"pred": "biota", "obj": "human", "type": "simple"},
                ],
                "type": "multi",
            }
        ]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_valid_dicts_input(self):
        input_str = (
            '{"Biota": {"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}}'
        )
        field = DictField(
            "Biota",
            AnonymousListField(
                "People", EmbeddedDictField(ScalarField("Name"), ScalarField("Age"))
            ),
        )
        expected_output_data = [
            {
                "pred": "biota",
                "obj": [
                    {
                        "pred": "biota",
                        "obj": [
                            {"pred": "name", "obj": "Bob", "type": "simple"},
                            {"pred": "age", "obj": 49, "type": "simple"},
                        ],
                        "type": "multi",
                    },
                    {
                        "pred": "biota",
                        "obj": [
                            {"pred": "name", "obj": "Sue", "type": "simple"},
                            {"pred": "age", "obj": 42, "type": "simple"},
                        ],
                        "type": "multi",
                    },
                ],
                "type": "multi",
            }
        ]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_invalid_input_missing_source_key(self):
        input_str = '{"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}'
        field = AnonymousListField(
            "Stuff", EmbeddedDictField(ScalarField("Name"), ScalarField("Age"))
        )
        input_data = json.loads(input_str)
        with self.assertRaises(ListFieldSourceKeyNotFoundException):
            field.parse(data=input_data, context={"parent_alti_key": "test_parent"})

    def test_invalid_input_not_list(self):
        input_str = '{"People": "foo"}'
        field = AnonymousListField(
            "People", EmbeddedDictField(ScalarField("Name"), ScalarField("Age"))
        )
        input_data = json.loads(input_str)
        with self.assertRaises(ListFieldValueNotAListException):
            field.parse(data=input_data, context={"parent_alti_key": "test_parent"})

    def test_optional(self):
        input_str = '{"Biota": {"Plants": ["tree", "fern"]}}'
        field = DictField(
            "Biota", AnonymousListField("Animals", EmbeddedScalarField(), optional=True)
        )
        expected_output_data = [{"pred": "biota", "obj": [], "type": "multi"}]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_allow_scalar(self):
        input_str = '{"Biota": {"Plants": "tree"}}'
        field = DictField(
            "Biota", AnonymousListField("Plants", EmbeddedScalarField(), allow_scalar=True)
        )
        expected_output_data = [
            {
                "pred": "biota",
                "obj": [{"pred": "biota", "obj": "tree", "type": "simple"}],
                "type": "multi",
            }
        ]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)
