import json
import mock
import os
from pathlib import Path
import tempfile
import time
from unittest import TestCase

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.auth.exceptions import AccountAuthException

import jinja2
from moto import mock_sts

TEST_EXT_ID = "foo"

TEST_ACCESSOR_FILE_CONTENT = {
    "multi_hop_accessors": [
        {
            "role_session_name": "altimeter-mb-a",
            "access_steps": [
                {
                    "account_id": "123456789012",
                    "external_id": "{{ env['TEST_EXT_ID'] }}",
                    "role_name": "OrganizationAccountAccessRole",
                },
                {"role_name": "OrganizationAccountAccessRole"},
            ],
        },
        {
            "role_session_name": "altimeter-mb-b",
            "access_steps": [
                {
                    "account_id": "123456789012",
                    "external_id": "{{ env['TEST_EXT_ID'] }}",
                    "role_name": "OrganizationAccountAccessRole",
                },
                {
                    "account_id": "123456789012",
                    "external_id": "{{ env['TEST_EXT_ID'] }}",
                    "role_name": "OrganizationAccountAccessRole",
                },
                {"role_name": "OrganizationAccountAccessRole"},
            ],
        },
    ]
}

TEST_ACCESSOR_DICT = {
    "multi_hop_accessors": [
        {
            "role_session_name": "altimeter-mb-a",
            "access_steps": [
                {
                    "account_id": "123456789012",
                    "external_id": TEST_EXT_ID,
                    "role_name": "OrganizationAccountAccessRole",
                },
                {"role_name": "OrganizationAccountAccessRole"},
            ],
        },
        {
            "role_session_name": "altimeter-mb-b",
            "access_steps": [
                {
                    "account_id": "123456789012",
                    "external_id": TEST_EXT_ID,
                    "role_name": "OrganizationAccountAccessRole",
                },
                {
                    "account_id": "123456789012",
                    "external_id": TEST_EXT_ID,
                    "role_name": "OrganizationAccountAccessRole",
                },
                {"role_name": "OrganizationAccountAccessRole"},
            ],
        },
    ]
}


class TestAccessor(TestCase):
    @mock.patch.dict(os.environ, {"TEST_EXT_ID": TEST_EXT_ID})
    @mock_sts
    def test_get_session(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            access_config_path = Path(tmp_dir, "access_config.json")
            with open(access_config_path, "w") as fp:
                fp.write(json.dumps(TEST_ACCESSOR_FILE_CONTENT))
            accessor = Accessor.from_file(filepath=access_config_path)
            session = accessor.get_session(account_id="123456789012")
            sts_client = session.client("sts")
            sts_account_id = sts_client.get_caller_identity()["Account"]
            self.assertEqual(sts_account_id, "123456789012")

    @mock.patch.dict(os.environ, {"TEST_EXT_ID": TEST_EXT_ID})
    @mock_sts
    def test_get_session_fall_through_failure(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            access_config_path = Path(tmp_dir, "access_config.json")
            with open(access_config_path, "w") as fp:
                fp.write(json.dumps(TEST_ACCESSOR_FILE_CONTENT))
            accessor = Accessor.from_file(filepath=access_config_path)
            with mock.patch(
                "altimeter.aws.auth.multi_hop_accessor.MultiHopAccessor.get_session"
            ) as mock_get_session:
                mock_get_session.side_effect = Exception("MockAuthFailure")
                with self.assertRaises(AccountAuthException):
                    accessor.get_session(account_id="123456789012")

    def test_from_dict(self):
        accessor = Accessor(**TEST_ACCESSOR_DICT)
        self.maxDiff = None
        self.assertDictEqual(
            accessor.dict(),
            {
                "multi_hop_accessors": [
                    {
                        "role_session_name": "altimeter-mb-a",
                        "access_steps": [
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": None,
                                "external_id": None,
                            },
                        ],
                    },
                    {
                        "role_session_name": "altimeter-mb-b",
                        "access_steps": [
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": None,
                                "external_id": None,
                            },
                        ],
                    },
                ],
                "credentials_cache": {"cache": {}},
                "cache_creds": True,
            },
        )

    def test_from_file_missing_env_args(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            access_config_path = Path(tmp_dir, "access_config.json")
            with open(access_config_path, "w") as fp:
                fp.write(json.dumps(TEST_ACCESSOR_FILE_CONTENT))
            with self.assertRaises(jinja2.exceptions.UndefinedError):
                Accessor.from_file(filepath=access_config_path)

    @mock.patch.dict(os.environ, {"TEST_EXT_ID": TEST_EXT_ID})
    def test_from_file_present_env_args(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            access_config_path = Path(tmp_dir, "access_config.json")
            with open(access_config_path, "w") as fp:
                fp.write(json.dumps(TEST_ACCESSOR_FILE_CONTENT))
            accessor = Accessor.from_file(filepath=access_config_path)
        self.assertDictEqual(
            accessor.dict(),
            {
                "multi_hop_accessors": [
                    {
                        "role_session_name": "altimeter-mb-a",
                        "access_steps": [
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": None,
                                "external_id": None,
                            },
                        ],
                    },
                    {
                        "role_session_name": "altimeter-mb-b",
                        "access_steps": [
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": None,
                                "external_id": None,
                            },
                        ],
                    },
                ],
                "credentials_cache": {"cache": {}},
                "cache_creds": True,
            },
        )

    @mock.patch.dict(os.environ, {"TEST_EXT_ID": TEST_EXT_ID})
    def test_from_file_without_cred_caching(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            access_config_path = Path(tmp_dir, "access_config.json")
            with open(access_config_path, "w") as fp:
                fp.write(json.dumps(TEST_ACCESSOR_FILE_CONTENT))
            accessor = Accessor.from_file(filepath=access_config_path, cache_creds=False)
        self.assertDictEqual(
            accessor.dict(),
            {
                "multi_hop_accessors": [
                    {
                        "role_session_name": "altimeter-mb-a",
                        "access_steps": [
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": None,
                                "external_id": None,
                            },
                        ],
                    },
                    {
                        "role_session_name": "altimeter-mb-b",
                        "access_steps": [
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": "123456789012",
                                "external_id": "foo",
                            },
                            {
                                "role_name": "OrganizationAccountAccessRole",
                                "account_id": None,
                                "external_id": None,
                            },
                        ],
                    },
                ],
                "credentials_cache": {"cache": {}},
                "cache_creds": False,
            },
        )

    def from_dict_to_dict(self):
        data = {
            "multi_hop_accessors": [
                {
                    "role_session_name": "altimeter-mb-a",
                    "access_steps": [
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": "123456789012",
                            "external_id": "foo",
                        },
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": None,
                            "external_id": None,
                        },
                    ],
                },
                {
                    "role_session_name": "altimeter-mb-b",
                    "access_steps": [
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": "123456789012",
                            "external_id": "foo",
                        },
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": "123456789012",
                            "external_id": "foo",
                        },
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": None,
                            "external_id": None,
                        },
                    ],
                },
            ],
            "credentials_cache": {"cache": {}},
        }
        self.assertDictEqual(data, Accessor(**data).dict())

    @mock.patch.dict(os.environ, {"TEST_EXT_ID": TEST_EXT_ID})
    def from_dict_to_dict_with_cache(self):
        in_five_min_epoch = int(time.time()) + 5 * 60
        data = {
            "multi_hop_accessors": [
                {
                    "role_session_name": "altimeter-mb-a",
                    "access_steps": [
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": "123456789012",
                            "external_id": "foo",
                        },
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": None,
                            "external_id": None,
                        },
                    ],
                },
                {
                    "role_session_name": "altimeter-mb-b",
                    "access_steps": [
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": "123456789012",
                            "external_id": "foo",
                        },
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": "12345678901",
                            "external_id": "foo",
                        },
                        {
                            "role_name": "OrganizationAccountAccessRole",
                            "account_id": None,
                            "external_id": None,
                        },
                    ],
                },
            ],
            "credentials_cache": {
                "cache": {
                    "123456789012:test_rn:test_rsn": {
                        "access_key_id": "test_aki",
                        "secret_access_key": "test_sak",
                        "session_token": "test_st",
                        "expiration": in_five_min_epoch,
                    },
                    "123456789012:test_rn2:test_rsn2": {
                        "access_key_id": "test_aki2",
                        "secret_access_key": "test_sak2",
                        "session_token": "test_st2",
                        "expiration": in_five_min_epoch,
                    },
                }
            },
        }
        self.assertDictEqual(data, Accessor(**data).dict())

    def test_str(self):
        accessor = Accessor(**TEST_ACCESSOR_DICT)
        self.assertEqual(
            str(accessor),
            (
                "accessor:altimeter-mb-a:OrganizationAccountAccessRole@123456789012,"
                "OrganizationAccountAccessRole@target,"
                "accessor:altimeter-mb-b:OrganizationAccountAccessRole@123456789012,"
                "OrganizationAccountAccessRole@123456789012,"
                "OrganizationAccountAccessRole@target"
            ),
        )
