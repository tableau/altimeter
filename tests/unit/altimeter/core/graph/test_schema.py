from unittest import TestCase

from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class TestSchemna(TestCase):
    def test_parse(self):
        schema = Schema(ScalarField("Key1"), ScalarField("Key2"))
        data = {"Key1": "Value1", "Key2": "Value2"}
        links = schema.parse(data, {})
        expected_link_data = [
            {"pred": "key1", "obj": "Value1", "type": "simple"},
            {"pred": "key2", "obj": "Value2", "type": "simple"},
        ]
        link_data = [link.to_dict() for link in links]
        self.assertEqual(expected_link_data, link_data)
