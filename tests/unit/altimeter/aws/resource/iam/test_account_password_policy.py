import unittest

from altimeter.core.graph.links import LinkCollection, SimpleLink
from altimeter.core.resource.resource import Resource
from altimeter.aws.resource.iam.account_password_policy import IAMAccountPasswordPolicyResourceSpec


class TestAccountPasswordPolicyResourceSpec(unittest.TestCase):
    def test_schema_parse(self):
        resource_arn = "arn:aws:iam:us-west-2:111122223333:account-password-policy/default"
        aws_resource_dict = {
            "MinimumPasswordLength": 12,
            "RequireSymbols": True,
            "RequireNumbers": True,
            "RequireUppercaseCharacters": True,
            "RequireLowercaseCharacters": True,
            "AllowUsersToChangePassword": True,
            "ExpirePasswords": True,
            "MaxPasswordAge": 90,
            "PasswordReusePrevention": 5,
            "HardExpiry": True,
        }

        link_collection = IAMAccountPasswordPolicyResourceSpec.schema.parse(
            data=aws_resource_dict, context={"account_id": "111122223333", "region": "us-west-2"}
        )
        resource = Resource(
            resource_id=resource_arn,
            type=IAMAccountPasswordPolicyResourceSpec.type_name,
            link_collection=link_collection,
        )

        expected_resource = Resource(
            resource_id="arn:aws:iam:us-west-2:111122223333:account-password-policy/default",
            type="account-password-policy",
            link_collection=LinkCollection(
                simple_links=(
                    SimpleLink(pred="minimum_password_length", obj=12),
                    SimpleLink(pred="require_symbols", obj=True),
                    SimpleLink(pred="require_numbers", obj=True),
                    SimpleLink(pred="require_uppercase_characters", obj=True),
                    SimpleLink(pred="require_lowercase_characters", obj=True),
                    SimpleLink(pred="allow_users_to_change_password", obj=True),
                    SimpleLink(pred="expire_passwords", obj=True),
                    SimpleLink(pred="max_password_age", obj=90),
                    SimpleLink(pred="password_reuse_prevention", obj=5),
                    SimpleLink(pred="hard_expiry", obj=True),
                )
            ),
        )
        self.assertEqual(resource, expected_resource)
