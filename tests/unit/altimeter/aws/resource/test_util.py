import json
from unittest import TestCase

from altimeter.aws.resource.util import (
    policy_doc_dict_to_sorted_str,
    deep_sort_dict,
    deep_sort_list,
)


class TestDeepSortDict(TestCase):
    def test_supported_datatypes(self):
        dct = {
            "xyz": [1234, 5123, 1.2, "foo", {"z": 1, "a": 2}],
            "qrs": 1234,
            "ijk": "qrst",
            "abc": {"___z": 691, "q": [15, 1, 29]},
        }
        expected_sorted_dict_json = '{"abc": {"___z": 691, "q": [1, 15, 29]}, "ijk": "qrst", "qrs": 1234, "xyz": ["foo", 1.2, 1234, 5123, {"a": 2, "z": 1}]}'

        sorted_dict = deep_sort_dict(dct)
        sorted_dict_json = json.dumps(sorted_dict)

        self.assertEqual(sorted_dict_json, expected_sorted_dict_json)

    def test_unsupported_datatypes(self):
        dct = {
            "xyz": [1234, 5123, 1.2, "foo", {"z": 1, "a": 2}],
            "y": set((1, 2, 3)),
            "qrs": 1234,
            "ijk": "qrst",
            "abc": {"___z": 691, "q": [15, 1, 29]},
        }
        with self.assertRaises(NotImplementedError):
            deep_sort_dict(dct)


class TestDeepSortList(TestCase):
    def test_supported_datatypes(self):
        lst = [
            "abcd",
            1234,
            -1234,
            "Abcd",
            {"zkey5": [1, 2, 3], "Key6": {"a": 1, "b": [9, 8, 7]}},
            [{"xyz": 123}, "abcd", 98766123, 3.141592653589793],
        ]
        expected_sorted_list_json = '["Abcd", "abcd", -1234, 1234, ["abcd", 3.141592653589793, 98766123, {"xyz": 123}], {"Key6": {"a": 1, "b": [7, 8, 9]}, "zkey5": [1, 2, 3]}]'

        sorted_list = deep_sort_list(lst)
        sorted_list_json = json.dumps(sorted_list)

        self.assertEqual(sorted_list_json, expected_sorted_list_json)

    def test_unsupported_datatypes(self):
        lst = [
            "abcd",
            set((1, 2, 3)),
            1234,
            -1234,
            "Abcd",
            {"zkey5": [1, 2, 3], "Key6": {"a": 1, "b": [9, 8, 7]}},
            [{"xyz": 123}, "abcd", 98766123, 3.141592653589793],
        ]
        with self.assertRaises(NotImplementedError):
            deep_sort_list(lst)


class TestPolicyDocDictToSortedStr(TestCase):
    def test(self):
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowViewAccountInfo",
                    "Effect": "Allow",
                    "Action": [
                        "iam:GetAccountPasswordPolicy",
                        "iam:GetAccountSummary",
                        "iam:ListVirtualMFADevices",
                    ],
                    "Resource": "*",
                },
                {
                    "Sid": "AllowManageOwnPasswords",
                    "Effect": "Allow",
                    "Action": ["iam:ChangePassword", "iam:GetUser"],
                    "Resource": "arn:aws:iam::*:user/${aws:username}",
                },
                {
                    "Sid": "AllowManageOwnAccessKeys",
                    "Effect": "Allow",
                    "Action": [
                        "iam:CreateAccessKey",
                        "iam:DeleteAccessKey",
                        "iam:ListAccessKeys",
                        "iam:UpdateAccessKey",
                    ],
                    "Resource": "arn:aws:iam::*:user/${aws:username}",
                },
                {
                    "Sid": "AllowManageOwnSigningCertificates",
                    "Effect": "Allow",
                    "Action": [
                        "iam:DeleteSigningCertificate",
                        "iam:ListSigningCertificates",
                        "iam:UpdateSigningCertificate",
                        "iam:UploadSigningCertificate",
                    ],
                    "Resource": "arn:aws:iam::*:user/${aws:username}",
                },
                {
                    "Sid": "AllowManageOwnSSHPublicKeys",
                    "Effect": "Allow",
                    "Action": [
                        "iam:DeleteSSHPublicKey",
                        "iam:GetSSHPublicKey",
                        "iam:ListSSHPublicKeys",
                        "iam:UpdateSSHPublicKey",
                        "iam:UploadSSHPublicKey",
                    ],
                    "Resource": "arn:aws:iam::*:user/${aws:username}",
                },
                {
                    "Sid": "AllowManageOwnGitCredentials",
                    "Effect": "Allow",
                    "Action": [
                        "iam:CreateServiceSpecificCredential",
                        "iam:DeleteServiceSpecificCredential",
                        "iam:ListServiceSpecificCredentials",
                        "iam:ResetServiceSpecificCredential",
                        "iam:UpdateServiceSpecificCredential",
                    ],
                    "Resource": "arn:aws:iam::*:user/${aws:username}",
                },
                {
                    "Sid": "AllowManageOwnVirtualMFADevice",
                    "Effect": "Allow",
                    "Action": ["iam:CreateVirtualMFADevice", "iam:DeleteVirtualMFADevice"],
                    "Resource": "arn:aws:iam::*:mfa/${aws:username}",
                },
                {
                    "Sid": "AllowManageOwnUserMFA",
                    "Effect": "Allow",
                    "Action": [
                        "iam:DeactivateMFADevice",
                        "iam:EnableMFADevice",
                        "iam:ListMFADevices",
                        "iam:ResyncMFADevice",
                    ],
                    "Resource": "arn:aws:iam::*:user/${aws:username}",
                },
                {
                    "Sid": "DenyAllExceptListedIfNoMFA",
                    "Effect": "Deny",
                    "NotAction": [
                        "iam:CreateVirtualMFADevice",
                        "iam:EnableMFADevice",
                        "iam:GetUser",
                        "iam:ListMFADevices",
                        "iam:ListVirtualMFADevices",
                        "iam:ResyncMFADevice",
                        "sts:GetSessionToken",
                    ],
                    "Resource": "*",
                    "Condition": {"BoolIfExists": {"aws:MultiFactorAuthPresent": "false"}},
                },
            ],
        }

        expected_policy_doc_sorted_str = '{"Statement": [{"Action": ["iam:ChangePassword", "iam:GetUser"], "Effect": "Allow", "Resource": "arn:aws:iam::*:user/${aws:username}", "Sid": "AllowManageOwnPasswords"}, {"Action": ["iam:CreateAccessKey", "iam:DeleteAccessKey", "iam:ListAccessKeys", "iam:UpdateAccessKey"], "Effect": "Allow", "Resource": "arn:aws:iam::*:user/${aws:username}", "Sid": "AllowManageOwnAccessKeys"}, {"Action": ["iam:CreateServiceSpecificCredential", "iam:DeleteServiceSpecificCredential", "iam:ListServiceSpecificCredentials", "iam:ResetServiceSpecificCredential", "iam:UpdateServiceSpecificCredential"], "Effect": "Allow", "Resource": "arn:aws:iam::*:user/${aws:username}", "Sid": "AllowManageOwnGitCredentials"}, {"Action": ["iam:CreateVirtualMFADevice", "iam:DeleteVirtualMFADevice"], "Effect": "Allow", "Resource": "arn:aws:iam::*:mfa/${aws:username}", "Sid": "AllowManageOwnVirtualMFADevice"}, {"Action": ["iam:DeactivateMFADevice", "iam:EnableMFADevice", "iam:ListMFADevices", "iam:ResyncMFADevice"], "Effect": "Allow", "Resource": "arn:aws:iam::*:user/${aws:username}", "Sid": "AllowManageOwnUserMFA"}, {"Action": ["iam:DeleteSSHPublicKey", "iam:GetSSHPublicKey", "iam:ListSSHPublicKeys", "iam:UpdateSSHPublicKey", "iam:UploadSSHPublicKey"], "Effect": "Allow", "Resource": "arn:aws:iam::*:user/${aws:username}", "Sid": "AllowManageOwnSSHPublicKeys"}, {"Action": ["iam:DeleteSigningCertificate", "iam:ListSigningCertificates", "iam:UpdateSigningCertificate", "iam:UploadSigningCertificate"], "Effect": "Allow", "Resource": "arn:aws:iam::*:user/${aws:username}", "Sid": "AllowManageOwnSigningCertificates"}, {"Action": ["iam:GetAccountPasswordPolicy", "iam:GetAccountSummary", "iam:ListVirtualMFADevices"], "Effect": "Allow", "Resource": "*", "Sid": "AllowViewAccountInfo"}, {"Condition": {"BoolIfExists": {"aws:MultiFactorAuthPresent": "false"}}, "Effect": "Deny", "NotAction": ["iam:CreateVirtualMFADevice", "iam:EnableMFADevice", "iam:GetUser", "iam:ListMFADevices", "iam:ListVirtualMFADevices", "iam:ResyncMFADevice", "sts:GetSessionToken"], "Resource": "*", "Sid": "DenyAllExceptListedIfNoMFA"}], "Version": "2012-10-17"}'

        policy_doc_sorted_str = policy_doc_dict_to_sorted_str(policy_doc)

        self.assertEqual(policy_doc_sorted_str, expected_policy_doc_sorted_str)
