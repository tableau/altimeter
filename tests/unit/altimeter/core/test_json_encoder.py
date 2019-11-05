import datetime
import json

from unittest import TestCase

from altimeter.core.json_encoder import json_encoder


class TestJsonEncoder(TestCase):
    def test_with_timestamp(self):
        now = datetime.datetime.now()
        data = {"foo": now}
        json_str = json.dumps(data, default=json_encoder)
        expected_str = f'{{"foo": "{now.isoformat()}"}}'
        self.assertEqual(json_str, expected_str)

    def test_with_unserializable(self):
        class Foo:
            boo = 1

        data = {"foo": Foo}
        with self.assertRaises(TypeError):
            json.dumps(data, default=json_encoder)
