"""Resource for KMSKeys"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.kms import KMSResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class KMSKeyResourceSpec(KMSResourceSpec):
    """Resource for KMS Keys"""

    type_name = "key"
    schema = Schema(ScalarField("KeyId"))

    @classmethod
    def list_from_aws(
        cls: Type["KMSKeyResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'key_1_arn': {key_1_dict},
             'key_2_arn': {key_2_dict},
             ...}

        Where the dicts represent results from list_keys."""
        keys = {}
        paginator = client.get_paginator("list_keys")
        for resp in paginator.paginate():
            for key in resp.get("Keys", []):
                resource_arn = key["KeyArn"]
                keys[resource_arn] = key
        return ListFromAWSResult(resources=keys)
