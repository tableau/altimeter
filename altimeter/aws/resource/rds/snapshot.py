"""Resource for RDS Snapshot"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.rds import RDSResourceSpec
from altimeter.aws.resource.rds.instance import RDSInstanceResourceSpec
from altimeter.aws.resource.kms.key import KMSKeyResourceSpec
from altimeter.core.graph.field.resource_link_field import TransientResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class RDSSnapshotResourceSpec(RDSResourceSpec):
    """Resource for RDS Snapshot"""

    type_name = "snapshot"
    schema = Schema(
        ScalarField("DBSnapshotIdentifier"),
        ScalarField("SnapshotCreateTime", optional=True),
        ScalarField("Engine"),
        ScalarField("AllocatedStorage"),
        ScalarField("Status"),
        TransientResourceLinkField("VpcId", VPCResourceSpec, optional=True),
        ScalarField("InstanceCreateTime"),
        ScalarField("SnapshotType"),
        ScalarField("PercentProgress"),
        ScalarField("Region", optional=True),
        ScalarField("Encrypted"),
        TransientResourceLinkField("KmsKeyID", KMSKeyResourceSpec, optional=True),
        ScalarField("Timezone", optional=True),
        ScalarField("IAMDatabaseAuthenticationEnabled"),
        TransientResourceLinkField("DBInstanceArn", RDSInstanceResourceSpec, value_is_id=True),
    )

    @classmethod
    def generate_instance_arn(
        cls: Type["RDSSnapshotResourceSpec"], account_id: str, region: str, resource_id: str
    ) -> str:
        """Generate an ARN for this resource, e.g. arn:aws:rds:<region>:<account>:db:<name>
        """
        return ":".join(
            (
                "arn",
                cls.provider_name,
                cls.service_name,
                region,
                account_id,
                cls.type_name,
                resource_id,
            )
        )

    @classmethod
    def list_from_aws(
        cls: Type["RDSSnapshotResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        dbinstances = {}
        paginator = client.get_paginator("describe_db_snapshots")
        for resp in paginator.paginate():
            for db in resp.get("DBSnapshots", []):
                resource_arn = db["DBSnapshotArn"]
                db["DBInstanceArn"] = cls.generate_instance_arn(
                    account_id, region, db["DBInstanceIdentifier"]
                )
                dbinstances[resource_arn] = db
        return ListFromAWSResult(resources=dbinstances)
