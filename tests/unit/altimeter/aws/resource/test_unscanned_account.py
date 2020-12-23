from unittest import TestCase

from altimeter.aws.resource.unscanned_account import UnscannedAccountResourceSpec
from altimeter.core.resource.resource_spec import ResourceSpec


class TestUnscannedAccountMultipleErrors(TestCase):
    def test(self):
        account_id = "012345678901"
        errors = ["foo", "boo"]
        unscanned_account_resource = UnscannedAccountResourceSpec.create_resource(
            account_id=account_id, errors=errors
        )
        resource = ResourceSpec.merge_resources("foo", [unscanned_account_resource])

        resource_dict = resource.to_dict()
        self.assertEqual(resource_dict["type"], "aws:unscanned-account")
        self.assertEqual(len(resource_dict["links"]), 2)
        self.assertEqual(resource_dict["links"][0], {'pred': 'account_id', 'obj': '012345678901', 'type': 'simple'})
        self.assertEqual(resource_dict["links"][1]["pred"], "error")
        self.assertEqual(resource_dict["links"][1]["type"], "simple")
        self.assertTrue(resource_dict["links"][1]["obj"].startswith("foo\nboo - "))