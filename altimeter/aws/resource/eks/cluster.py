"""Resource for Clusters"""
from typing import Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.eks import EKSResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class EKSClusterResourceSpec(EKSResourceSpec):
    """Resource for Clusters"""

    type_name = "cluster"
    schema = Schema(ScalarField("Name"),)

    @classmethod
    def list_from_aws(
        cls: Type["EKSClusterResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'cluster_1_arn': {cluster_1_dict},
             'cluster_2_arn': {cluster_2_dict},
             ...}

        Where the dicts represent results from list_clusters."""
        clusters = {}
        try:
            paginator = client.get_paginator("list_clusters")
            for resp in paginator.paginate():
                for cluster_name in resp.get("clusters", []):
                    resource_arn = cls.generate_arn(
                        account_id=account_id, region=region, resource_id=cluster_name
                    )
                    clusters[resource_arn] = {"Name": cluster_name}
        except ClientError as c_e:
            response_error = getattr(c_e, "response", {}).get("Error", {})
            error_code = response_error.get("Code", "")
            if error_code != "AccessDeniedException":
                raise c_e
            error_msg = response_error.get("Message", "")
            if error_msg != f"Account {account_id} is not authorized to use this service":
                raise c_e
        return ListFromAWSResult(resources=clusters)
