"""Resource for IAM Users"""
from typing import Any, List, Dict, Type, TypeVar

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


T = TypeVar("T", bound="IAMUserResourceSpec")


class IAMUserResourceSpec(IAMResourceSpec):
    """Resource for IAM Users"""

    type_name = "user"
    schema = Schema(
        ScalarField("UserName", "name"),
        ScalarField("UserId"),
        ScalarField("CreateDate"),
        ListField(
            "AccessKeys",
            EmbeddedDictField(
                ScalarField("AccessKeyId"), ScalarField("Status"), ScalarField("CreateDate")
            ),
            optional=True,
            alti_key="access_key",
        ),
        ListField(
            "MfaDevices",
            EmbeddedDictField(ScalarField("SerialNumber"), ScalarField("EnableDate")),
            optional=True,
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type[T], client: BaseClient, account_id: str, region: str
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
                    access_keys += access_keys_resp["AccessKeyMetadata"]
                user["AccessKeys"] = access_keys
                mfa_devices_paginator = client.get_paginator("list_mfa_devices")
                mfa_devices: List[Dict[str, Any]] = []
                for mfa_devices_resp in mfa_devices_paginator.paginate(UserName=user_name):
                    mfa_devices += mfa_devices_resp["MFADevices"]
                    user["MfaDevices"] = mfa_devices
                users[resource_arn] = user
        return ListFromAWSResult(resources=users)
