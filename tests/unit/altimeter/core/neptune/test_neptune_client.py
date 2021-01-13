from unittest import TestCase

from altimeter.core.neptune.client import (
    get_required_tag_value,
    NeptuneEndpoint,
    AltimeterNeptuneClient,
)
from altimeter.core.neptune.exceptions import (
    NeptuneNoGraphsFoundException,
    NeptuneLoadGraphException,
)


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
        expected_sparql_endpoint = "https://host:5555/sparql"

        sparql_endpoint = endpoint.get_sparql_endpoint()
        self.assertEqual(expected_sparql_endpoint, sparql_endpoint)

    def test_get_sparql_endpoint_ssl_true(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_sparql_endpoint = "https://host:5555/sparql"

        sparql_endpoint = endpoint.get_sparql_endpoint(ssl=True)
        self.assertEqual(expected_sparql_endpoint, sparql_endpoint)

    def test_get_sparql_endpoint_ssl_false(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_sparql_endpoint = "http://host:5555/sparql"

        sparql_endpoint = endpoint.get_sparql_endpoint(ssl=False)
        self.assertEqual(expected_sparql_endpoint, sparql_endpoint)

    def test_get_loader_endpoint(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_loader_endpoint = "https://host:5555/loader"

        loader_endpoint = endpoint.get_loader_endpoint()
        self.assertEqual(expected_loader_endpoint, loader_endpoint)

    def test_get_loader_endpoint_ssl_true(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_loader_endpoint = "https://host:5555/loader"

        loader_endpoint = endpoint.get_loader_endpoint(ssl=True)
        self.assertEqual(expected_loader_endpoint, loader_endpoint)

    def test_get_loader_endpoint_ssl_false(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_loader_endpoint = "http://host:5555/loader"

        loader_endpoint = endpoint.get_loader_endpoint(ssl=False)
        self.assertEqual(expected_loader_endpoint, loader_endpoint)

    def test_get_gremlin_endpoint(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_loader_endpoint = "wss://host:5555/gremlin"

        loader_endpoint = endpoint.get_gremlin_endpoint()
        self.assertEqual(expected_loader_endpoint, loader_endpoint)

    def test_get_gremlin_endpoint_ssl_true(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_loader_endpoint = "wss://host:5555/gremlin"

        loader_endpoint = endpoint.get_gremlin_endpoint(ssl=True)
        self.assertEqual(expected_loader_endpoint, loader_endpoint)

    def test_get_gremlin_endpoint_ssl_false(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        expected_loader_endpoint = "ws://host:5555/gremlin"

        loader_endpoint = endpoint.get_gremlin_endpoint(ssl=False)
        self.assertEqual(expected_loader_endpoint, loader_endpoint)


class TestAltimeterNeptuneClient(TestCase):
    def test_connect_to_gremlin_ssl_enabled(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1")
        client = AltimeterNeptuneClient(0, endpoint)
        g, conn = client.connect_to_gremlin()

        self.assertEqual("wss://host:5555/gremlin", conn.url)

    def test_connect_to_gremlin_ssl_disabled(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1", ssl=False)
        client = AltimeterNeptuneClient(0, endpoint)
        g, conn = client.connect_to_gremlin()

        self.assertEqual("ws://host:5555/gremlin", conn.url)

    def test_parse_arn(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1", ssl=False)
        client = AltimeterNeptuneClient(0, endpoint)
        expected_dict = {
            "arn": "arn",
            "partition": "aws",
            "service": "ec2",
            "region": "us-east-1",
            "account": "123",
            "resource": "vpc-123",
            "resource_type": "vpc",
        }
        actual_dict = client.parse_arn("arn:aws:ec2:us-east-1:123:vpc/vpc-123")

        self.assertDictEqual(expected_dict, actual_dict)

    def test_parse_arn_resource_only(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1", ssl=False)
        client = AltimeterNeptuneClient(0, endpoint)
        actual_dict = client.parse_arn("test")

        self.assertEqual("test", actual_dict["resource"])

    def test_parse_arn_resource_type(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1", ssl=False)
        client = AltimeterNeptuneClient(0, endpoint)
        expected_dict = {
            "arn": "arn",
            "partition": "aws",
            "service": "ec2",
            "region": "us-east-1",
            "account": "123",
            "resource": "vpc-123",
            "resource_type": "vpc",
        }
        actual_dict = client.parse_arn("arn:aws:ec2:us-east-1:123:vpc:vpc-123")

        self.assertDictEqual(expected_dict, actual_dict)

    def test_parse_arn_resource_type_ami(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1", ssl=False)
        client = AltimeterNeptuneClient(0, endpoint)
        expected_dict = {
            "arn": "arn",
            "partition": "aws",
            "service": "ec2",
            "region": "us-east-1",
            "account": "123",
            "resource": "ami",
            "resource_type": "ami",
        }
        actual_dict = client.parse_arn("arn:aws:ec2:us-east-1:123:ami:ami-123")

        self.assertDictEqual(expected_dict, actual_dict)

    def test_write_to_neptune_lpg_graph(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1", ssl=False)
        client = AltimeterNeptuneClient(0, endpoint)
        # An exception is expected here since there is no graph to write to
        with self.assertRaises(NeptuneLoadGraphException):
            client.write_to_neptune_lpg(
                {"vertices": [{"~id": "123", "~label": "test"}], "edges": []}, ""
            )

    def test_write_to_neptune_lpg_no_graph(self):
        endpoint = NeptuneEndpoint(host="host", port=5555, region="us-east-1", ssl=False)
        client = AltimeterNeptuneClient(0, endpoint)
        with self.assertRaises(NeptuneNoGraphsFoundException):
            client.write_to_neptune_lpg({}, "")
