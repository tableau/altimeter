"""Resource for IAM Users"""
import copy
from typing import Any, List, Dict, Type

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
                user_name = user["UserName"]
                access_keys_paginator = client.get_paginator("list_access_keys")
                access_keys: List[Dict[str, Any]] = []
                for access_keys_resp in access_keys_paginator.paginate(UserName=user_name):
                    for resp_access_key in access_keys_resp["AccessKeyMetadata"]:
                        access_key = copy.deepcopy(resp_access_key)
                        access_key_id = access_key["AccessKeyId"]
                        last_used_resp = client.get_access_key_last_used(AccessKeyId=access_key_id)
                        access_key["AccessKeyLastUsed"] = last_used_resp["AccessKeyLastUsed"]
                        access_keys.append(access_key)
                user["AccessKeys"] = access_keys
                mfa_devices_paginator = client.get_paginator("list_mfa_devices")
                mfa_devices: List[Dict[str, Any]] = []
                for mfa_devices_resp in mfa_devices_paginator.paginate(UserName=user_name):
                    mfa_devices += mfa_devices_resp["MFADevices"]
                    user["MfaDevices"] = mfa_devices
                try:
                    login_profile_resp = client.get_login_profile(UserName=user_name)
                    user["LoginProfile"] = login_profile_resp["LoginProfile"]
                except ClientError as c_e:
                    if "NoSuchEntity" not in str(c_e):
                        raise c_e
                users[resource_arn] = user
        return ListFromAWSResult(resources=users)
