from unittest import TestCase

from altimeter.aws.resource.unscanned_account import UnscannedAccountResourceSpec
from altimeter.core.graph.links import LinkCollection, SimpleLink
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec


class TestUnscannedAccountMultipleErrors(TestCase):
    def test(self):
        account_id = "012345678901"
        errors = ["foo", "boo"]
        unscanned_account_resource = UnscannedAccountResourceSpec.create_resource(
            account_id=account_id, errors=errors
        )
        resource = ResourceSpec.merge_resources("foo", [unscanned_account_resource])

        self.assertEqual(resource.resource_id, "foo")
        self.assertEqual(resource.type, "aws:unscanned-account")
        self.assertEqual(len(resource.link_collection.simple_links), 2)
        self.assertEqual(
            resource.link_collection.simple_links[0],
            SimpleLink(pred="account_id", obj="012345678901"),
        )
        self.assertEqual(resource.link_collection.simple_links[1].pred, "error")
        self.assertTrue(resource.link_collection.simple_links[1].obj.startswith("foo\nboo - "))
