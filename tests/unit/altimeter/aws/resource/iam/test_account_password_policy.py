import unittest

from altimeter.core.resource.resource import Resource
from altimeter.aws.resource.iam.account_password_policy import IAMAccountPasswordPolicyResourceSpec


class TestAccountPasswordPolicyResourceSpec(unittest.TestCase):
    def test_schema_parse(self):
        resource_arn = "arn:aws:iam:us-west-2:111122223333:account-password-policy/default"
        aws_resource_dict = {
            'MinimumPasswordLength': 12, 'RequireSymbols': True, 'RequireNumbers': True,
            'RequireUppercaseCharacters': True, 'RequireLowercaseCharacters': True, 'AllowUsersToChangePassword': True,
            'ExpirePasswords': True, 'MaxPasswordAge': 90, 'PasswordReusePrevention': 5, 'HardExpiry': True
        }

        links = IAMAccountPasswordPolicyResourceSpec.schema.parse(
            data=aws_resource_dict, context={"account_id": "111122223333", "region": "us-west-2"}
        )
        resource = Resource(
            resource_id=resource_arn, type_name=IAMAccountPasswordPolicyResourceSpec.type_name, links=links
        )
        alti_resource_dict = resource.to_dict()

        expected_alti_resource_dict = {'type': 'account-password-policy',
                                       'links': [{'pred': 'minimum_password_length', 'obj': 12, 'type': 'simple'},
                                                 {'pred': 'require_symbols', 'obj': True, 'type': 'simple'},
                                                 {'pred': 'require_numbers', 'obj': True, 'type': 'simple'}, {
                                                     'pred': 'require_uppercase_characters', 'obj': True,
                                                     'type': 'simple'
                                                 }, {
                                                     'pred': 'require_lowercase_characters', 'obj': True,
                                                     'type': 'simple'
                                                 }, {
                                                     'pred': 'allow_users_to_change_password', 'obj': True,
                                                     'type': 'simple'
                                                 }, {'pred': 'expire_passwords', 'obj': True, 'type': 'simple'},
                                                 {'pred': 'max_password_age', 'obj': 90, 'type': 'simple'},
                                                 {'pred': 'password_reuse_prevention', 'obj': 5, 'type': 'simple'},
                                                 {'pred': 'hard_expiry', 'obj': True, 'type': 'simple'}]
        }

        self.assertDictEqual(alti_resource_dict, expected_alti_resource_dict)
