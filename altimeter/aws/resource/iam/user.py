"""Resource for IAM Users"""
import copy
from typing import Any, Dict, List, Optional, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.core.graph.field.dict_field import AnonymousDictField, DictField, EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class IAMUserResourceSpec(IAMResourceSpec):
    """Resource for IAM Users"""

    type_name = "user"
    schema = Schema(
        ScalarField("UserName", "name"),
        ScalarField("UserId"),
        ScalarField("CreateDate"),
        ScalarField("PasswordLastUsed", optional=True),
        ListField(
            "AccessKeys",
            EmbeddedDictField(
                ScalarField("AccessKeyId"),
                ScalarField("Status"),
                ScalarField("CreateDate"),
                AnonymousDictField("AccessKeyLastUsed", ScalarField("LastUsedDate", optional=True)),
            ),
            optional=True,
            alti_key="access_key",
        ),
        DictField(
            "LoginProfile",
            ScalarField("CreateDate"),
            ScalarField("PasswordResetRequired"),
            optional=True,
        ),
        ListField(
            "MfaDevices",
            EmbeddedDictField(ScalarField("SerialNumber"), ScalarField("EnableDate")),
            optional=True,
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["IAMUserResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'user_1_arn': {user_1_dict},
             'user_2_arn': {user_2_dict},
             ...}

        Where the dicts represent results from list_users and additional info per user from
        list_targets_by_user."""
        users = {}
        paginator = client.get_paginator("list_users")
        for resp in paginator.paginate():
            for user in resp.get("Users", []):
                resource_arn = user["Arn"]
                username = user["UserName"]
                try:
                    user["AccessKeys"] = cls.get_user_access_keys(client=client, username=username)
                    user["MfaDevices"] = cls.get_user_mfa_devices(client=client, username=username)
                    login_profile = cls.get_user_login_profile(client=client, username=username)
                    if login_profile is not None:
                        user["LoginProfile"] = login_profile
                    users[resource_arn] = user
                except ClientError as c_e:
                    error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                    if error_code != "NoSuchEntity":
                        raise c_e
        return ListFromAWSResult(resources=users)

    @classmethod
    def get_user_access_keys(
        cls: Type["IAMUserResourceSpec"], client: BaseClient, username: str
    ) -> List[Dict[str, Any]]:
        access_keys: List[Dict[str, Any]] = []
        access_keys_paginator = client.get_paginator("list_access_keys")
        for access_keys_resp in access_keys_paginator.paginate(UserName=username):
            for resp_access_key in access_keys_resp["AccessKeyMetadata"]:
                access_key = copy.deepcopy(resp_access_key)
                access_key_id = access_key["AccessKeyId"]
                try:
                    access_key_last_used = cls.get_access_key_last_used(
                        client=client, access_key_id=access_key_id
                    )
                    access_key["AccessKeyLastUsed"] = access_key_last_used
                    access_keys.append(access_key)
                except ClientError as c_e:
                    error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                    if error_code != "AccessDenied":
                        raise c_e
        return access_keys

    @classmethod
    def get_access_key_last_used(
        cls: Type["IAMUserResourceSpec"], client: BaseClient, access_key_id: str
    ) -> Dict[str, Any]:
        resp = client.get_access_key_last_used(AccessKeyId=access_key_id)
        return resp["AccessKeyLastUsed"]

    @classmethod
    def get_user_mfa_devices(
        cls: Type["IAMUserResourceSpec"], client: BaseClient, username: str
    ) -> List[Dict[str, Any]]:
        mfa_devices: List[Dict[str, Any]] = []
        mfa_devices_paginator = client.get_paginator("list_mfa_devices")
        for mfa_devices_resp in mfa_devices_paginator.paginate(UserName=username):
            mfa_devices += mfa_devices_resp["MFADevices"]
        return mfa_devices

    @classmethod
    def get_user_login_profile(
        cls: Type["IAMUserResourceSpec"], client: BaseClient, username: str
    ) -> Optional[Dict[str, Any]]:
        try:
            login_profile_resp = client.get_login_profile(UserName=username)
            return login_profile_resp["LoginProfile"]
        except ClientError as c_e:
            error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
            if error_code != "NoSuchEntity":
                raise c_e
        return None
