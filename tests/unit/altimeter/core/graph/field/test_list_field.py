import json
from unittest import TestCase

from altimeter.core.graph.exceptions import (
    ListFieldSourceKeyNotFoundException,
    ListFieldValueNotAListException,
)
from altimeter.core.graph.field.dict_field import EmbeddedDictField, DictField
from altimeter.core.graph.field.list_field import AnonymousListField, ListField
from altimeter.core.graph.field.scalar_field import EmbeddedScalarField, ScalarField
from altimeter.core.graph.links import LinkCollection, MultiLink, SimpleLink


class TestListField(TestCase):
    def test_valid_strings_input(self):
        input_str = '{"Animals": ["cow", "pig", "human"]}'
        field = ListField("Animals", EmbeddedScalarField())

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            simple_links=(
                SimpleLink(pred="animals", obj="cow"),
                SimpleLink(pred="animals", obj="pig"),
                SimpleLink(pred="animals", obj="human"),
            ),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_valid_dicts_input(self):
        input_str = '{"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}'
        field = ListField("People", EmbeddedDictField(ScalarField("Name"), ScalarField("Age")))

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            multi_links=(
                MultiLink(
                    pred="people",
                    obj=LinkCollection(
                        simple_links=(
                            SimpleLink(pred="name", obj="Bob"),
                            SimpleLink(pred="age", obj=49),
                        ),
                    ),
                ),
                MultiLink(
                    pred="people",
                    obj=LinkCollection(
                        simple_links=(
                            SimpleLink(pred="name", obj="Sue"),
                            SimpleLink(pred="age", obj=42),
                        ),
                    ),
                ),
            ),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_valid_dicts_input_with_alti_key(self):
        input_str = '{"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}'
        field = ListField(
            "People", EmbeddedDictField(ScalarField("Name"), ScalarField("Age")), alti_key="person"
        )

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            multi_links=(
                MultiLink(
                    pred="person",
                    obj=LinkCollection(
                        simple_links=(
                            SimpleLink(pred="name", obj="Bob"),
                            SimpleLink(pred="age", obj=49),
                        ),
                    ),
                ),
                MultiLink(
                    pred="person",
                    obj=LinkCollection(
                        simple_links=(
                            SimpleLink(pred="name", obj="Sue"),
                            SimpleLink(pred="age", obj=42),
                        ),
                    ),
                ),
            ),
        )
        self.assertEqual(link_collection, expected_link_collection)

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

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        self.assertEqual(link_collection, LinkCollection())

    def test_allow_scalar(self):
        input_str = '{"People": "bob"}'
        field = ListField("People", EmbeddedScalarField(), alti_key="person", allow_scalar=True)

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            simple_links=(SimpleLink(pred="person", obj="bob"),),
        )
        self.assertEqual(link_collection, expected_link_collection)


class TestAnonymousListField(TestCase):
    def test_valid_strings_input(self):
        input_str = '{"Biota": {"Animals": ["cow", "pig", "human"], "Plants": ["tree", "fern"]}}'
        field = DictField("Biota", AnonymousListField("Animals", EmbeddedScalarField()))

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            multi_links=(
                MultiLink(
                    pred="biota",
                    obj=LinkCollection(
                        simple_links=(
                            SimpleLink(pred="biota", obj="cow"),
                            SimpleLink(pred="biota", obj="pig"),
                            SimpleLink(pred="biota", obj="human"),
                        )
                    ),
                ),
            )
        )
        self.assertEqual(link_collection, expected_link_collection)

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

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            multi_links=(
                MultiLink(
                    pred="biota",
                    obj=LinkCollection(
                        multi_links=(
                            MultiLink(
                                pred="biota",
                                obj=LinkCollection(
                                    simple_links=(
                                        SimpleLink(pred="name", obj="Bob"),
                                        SimpleLink(pred="age", obj=49),
                                    ),
                                ),
                            ),
                            MultiLink(
                                pred="biota",
                                obj=LinkCollection(
                                    simple_links=(
                                        SimpleLink(pred="name", obj="Sue"),
                                        SimpleLink(pred="age", obj=42),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            )
        )
        self.assertEqual(link_collection, expected_link_collection)

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

        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            multi_links=(MultiLink(pred="biota", obj=LinkCollection()),),
        )
        self.assertEqual(link_collection, expected_link_collection)

    def test_allow_scalar(self):
        input_str = '{"Biota": {"Plants": "tree"}}'
        field = DictField(
            "Biota", AnonymousListField("Plants", EmbeddedScalarField(), allow_scalar=True)
        )
        input_data = json.loads(input_str)
        link_collection = field.parse(data=input_data, context={})

        expected_link_collection = LinkCollection(
            multi_links=(
                MultiLink(
                    pred="biota",
                    obj=LinkCollection(simple_links=(SimpleLink(pred="biota", obj="tree"),),),
                ),
            )
        )
        self.assertEqual(expected_link_collection, link_collection)
