import json
from unittest import TestCase

from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.field.exceptions import TagsFieldMissingTagsKeyException


class TestTagsField(TestCase):
    def test_valid_input(self):
        input_str = (
            '{"Tags": [{"Key": "tag1", "Value": "value1"}, {"Key": "tag2", "Value": "value2"}]}'
        )
        field = TagsField()
        expected_output_data = [
            {"pred": "tag1", "obj": "value1", "type": "tag"},
            {"pred": "tag2", "obj": "value2", "type": "tag"},
        ]

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)

    def test_invalid_input(self):
        input_str = (
            '{"Mags": [{"Key": "tag1", "Value": "value1"}, {"Key": "tag2", "Value": "value2"}]}'
        )
        field = TagsField(optional=False)

        input_data = json.loads(input_str)
        with self.assertRaises(TagsFieldMissingTagsKeyException):
            field.parse(data=input_data, context={})

    def test_optional(self):
        input_str = '{"NoTagsHere": []}'
        field = TagsField(optional=True)
        expected_output_data = []

        input_data = json.loads(input_str)
        links = field.parse(data=input_data, context={})
        output_data = [link.to_dict() for link in links]
        self.assertCountEqual(output_data, expected_output_data)
