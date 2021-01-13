from unittest import TestCase

from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.links import LinkCollection, SimpleLink
from altimeter.core.graph.schema import Schema


class TestSchema(TestCase):
    def test_parse(self):
        schema = Schema(ScalarField("Key1"), ScalarField("Key2"))
        data = {"Key1": "Value1", "Key2": "Value2"}
        link_collection = schema.parse(data, {})
        expected_link_collection = LinkCollection(
            simple_links=(
                SimpleLink(pred="key1", obj="Value1"),
                SimpleLink(pred="key2", obj="Value2"),
            )
        )
        self.assertEqual(link_collection, expected_link_collection)
