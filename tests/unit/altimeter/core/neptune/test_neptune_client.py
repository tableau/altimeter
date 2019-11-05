from unittest import TestCase

from altimeter.core.neptune.client import get_required_tag_value, NeptuneEndpoint


class TestGetRequiredTagValue(TestCase):
    def test_with_present_key(self):
        tag_set = [{"Key": "name", "Value": "boo"}]
        expected_value = "boo"

        value = get_required_tag_value(tag_set, "name")
        self.assertEqual(expected_value, value)

    def test_with_absent_key(self):
        tag_set = [{"Key": "name", "Value": "boo"}]

        with self.assertRaises(ValueError):
            get_required_tag_value(tag_set, "boo")


class TestNeptuneEndpoint(TestCase):
    def test_get_endpoint_str(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_endpoint_str = "host:5555"

        endpoint_str = endpoint.get_endpoint_str()
        self.assertEqual(expected_endpoint_str, endpoint_str)

    def test_get_sparql_endpoint(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_sparql_endpoint = "http://host:5555/sparql"

        sparql_endpoint = endpoint.get_sparql_endpoint()
        self.assertEqual(expected_sparql_endpoint, sparql_endpoint)

    def test_get_loader_endpoint(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_loader_endpoint = "http://host:5555/loader"

        loader_endpoint = endpoint.get_loader_endpoint()
        self.assertEqual(expected_loader_endpoint, loader_endpoint)
