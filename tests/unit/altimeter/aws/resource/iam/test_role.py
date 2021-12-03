import json
from unittest import TestCase
from unittest.mock import patch
import boto3
from botocore.exceptions import ClientError
from moto import mock_iam
from altimeter.aws.resource.iam.role import IAMRoleResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.aws.resource.util import policy_doc_dict_to_sorted_str


class TestIAMRole(TestCase):
    @mock_iam
    def test_disappearing_role_race_condition(self):
        account_id = "123456789012"
        role_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()
        client = session.client("iam")
        client.create_role(RoleName=role_name, AssumeRolePolicyDocument="{}")

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.role.get_attached_role_policies"
        ) as mock_get_group_users:
            mock_get_group_users.side_effect = ClientError(
                operation_name="ListAttachedRolePolicies",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"The role with name {role_name} cannot be found.",
                    }
                },
            )
            resources = IAMRoleResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])

    @mock_iam
    def test_get_embedded_policy(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        role_name = "foo"

        role_policy = "foo_policy"
        policy_document = """{
            "Statement": [
                {
                    "Action": ["sts:assumeRole"],
                    "Effect": "Allow", "Resource": ["*"]
                }
            ],
            "Version": "2012-10-17"
        }"""

        role_policy2 = "foo_policy2"
        policy_document2 = """{
            "Statement": [
                {
                    "Action": ["sqs:queue"],
                    "Effect": "Allow", "Resource": ["*"]
                }
            ],
            "Version": "2012-10-17"
        }"""

        assume_role_policy_document = """{
            "Version": "2012-10-17",
            "Statement": [
                {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
                }
            ]
        }"""
        session = boto3.Session()
        client = session.client("iam")
        client.create_role(RoleName=role_name, AssumeRolePolicyDocument=assume_role_policy_document)
        iam = boto3.resource("iam")
        policy = iam.RolePolicy(role_name, role_policy)
        policy.put(PolicyDocument=policy_document)
        policy = iam.RolePolicy(role_name, role_policy2)
        policy.put(PolicyDocument=policy_document2)

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        resources = IAMRoleResourceSpec.scan(scan_accessor=scan_accessor)
        embedded_resources_links = [
            link
            for link in resources[0].link_collection.multi_links
            if link.pred == "embedded_policy"
        ]
        self.assertEqual(len(embedded_resources_links), 2)
        # First policy.
        embedded_resources_link = embedded_resources_links[0]
        self.assertTrue(
            compare_embedded_policy(embedded_resources_link, role_policy, policy_document)
        )
        # Second policy.
        embedded_resources_link = embedded_resources_links[1]
        self.assertTrue(
            compare_embedded_policy(embedded_resources_link, role_policy2, policy_document2)
        )


def compare_embedded_policy(source_policy, expected_policy_name, expected_policy_document):
    if source_policy.pred != "embedded_policy":
        return False
    if len(source_policy.obj.simple_links) != 2:
        return False
    embedded_policy = source_policy.obj.simple_links[0]
    if embedded_policy.pred != "policy_name":
        return False
    if embedded_policy.obj != expected_policy_name:
        return False
    embedded_policy_document = source_policy.obj.simple_links[1]
    if embedded_policy_document.pred != "policy_document":
        return False
    got_policy_document = policy_doc_dict_to_sorted_str(json.loads(embedded_policy_document.obj))
    expected_policy_document = policy_doc_dict_to_sorted_str(json.loads(expected_policy_document))
    return got_policy_document == expected_policy_document
