"""Resource for RDS"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2.security_group import SecurityGroupResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.rds import RDSResourceSpec
from altimeter.aws.resource.kms.key import KMSKeyResourceSpec
from altimeter.core.graph.field.dict_field import (
    AnonymousDictField,
    AnonymousEmbeddedDictField,
    EmbeddedDictField,
)
from altimeter.core.graph.field.list_field import AnonymousListField, ListField
from altimeter.core.graph.field.resource_link_field import TransientResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema
from altimeter.core.log import Logger


class RDSInstanceResourceSpec(RDSResourceSpec):
    """Resource for RDS"""

    type_name = "db"
    schema = Schema(
        TagsField(),
        ScalarField("DBInstanceIdentifier"),
        ScalarField("DBInstanceClass"),
        ScalarField("Engine"),
        ScalarField("DBInstanceStatus"),
        ScalarField("DBName", optional=True),
        AnonymousDictField(
            "Endpoint",
            ScalarField("Address", alti_key="endpoint_address", optional=True),
            ScalarField("Port", alti_key="endpoint_port"),
            ScalarField("HostedZoneId", alti_key="endpoint_hosted_zone", optional=True),
            optional=True,
        ),
        AnonymousDictField(
            "ListenerEndpoint",
            ScalarField("Address", alti_key="listener_address"),
            ScalarField("Port", alti_key="listener_port"),
            ScalarField("HostedZoneId", alti_key="listener_hosted_zone", optional=True),
            optional=True,
        ),
        ScalarField("InstanceCreateTime", optional=True),
        ScalarField("BackupRetentionPeriod"),
        AnonymousListField(
            "VpcSecurityGroups",
            AnonymousEmbeddedDictField(
                TransientResourceLinkField(
                    "VpcSecurityGroupId", SecurityGroupResourceSpec, optional=True
                )
            ),
        ),
        ScalarField("AvailabilityZone", optional=True),
        AnonymousDictField(
            "DBSubnetGroup", TransientResourceLinkField("VpcId", VPCResourceSpec), optional=True
        ),
        ScalarField("MultiAZ"),
        ScalarField("PubliclyAccessible"),
        ListField("StatusInfos", EmbeddedDictField(ScalarField("Status")), optional=True),
        ScalarField("StorageType"),
        ScalarField("StorageEncrypted"),
        TransientResourceLinkField("KmsKeyId", KMSKeyResourceSpec, optional=True, value_is_id=True),
        ScalarField("DbiResourceId"),
        ScalarField("Timezone", optional=True),
        ScalarField("IAMDatabaseAuthenticationEnabled"),
        ScalarField("PerformanceInsightsEnabled", optional=True),
        ScalarField("PerformanceInsightsRetentionPeriod", optional=True),
        ScalarField("DeletionProtection"),
        ListField(
            "Backup",
            EmbeddedDictField(
                AnonymousDictField(
                    "RestoreWindow",
                    ScalarField("EarliestTime", alti_key="earliest_restore_time", optional=True),
                    optional=True,
                ),
                AnonymousDictField(
                    "RestoreWindow",
                    ScalarField("LatestTime", alti_key="latest_restore_time", optional=True),
                    optional=True,
                ),
                ScalarField("AllocatedStorage"),
                ScalarField("Status"),
                ScalarField("AvailabilityZone", optional=True),
                ScalarField("Engine"),
                ScalarField("EngineVersion"),
                ScalarField("Encrypted"),
                ScalarField("StorageType"),
                TransientResourceLinkField(
                    "KmsKeyId", KMSKeyResourceSpec, optional=True, value_is_id=True
                ),
            ),
            optional=True,
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["RDSInstanceResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        logger = Logger()
        dbinstances = {}
        paginator = client.get_paginator("describe_db_instances")
        for resp in paginator.paginate():
            for db in resp.get("DBInstances", []):
                resource_arn = db["DBInstanceArn"]
                db["Tags"] = client.list_tags_for_resource(ResourceName=resource_arn).get(
                    "TagList", []
                )
                db["Backup"] = []
                dbinstances[resource_arn] = db

        backup_paginator = client.get_paginator("describe_db_instance_automated_backups")
        for resp in backup_paginator.paginate():
            for backup in resp.get("DBInstanceAutomatedBackups", []):
                if backup["DBInstanceArn"] in dbinstances:
                    dbinstances[backup["DBInstanceArn"]]["Backup"].append(backup)
                else:
                    logger.info(
                        event=AWSLogEvents.ScanAWSResourcesNonFatalError,
                        msg=(
                            f'Unable to find matching DB Instance {backup["DBInstanceArn"]} '
                            "(Possible Deletion)"
                        ),
                    )
        return ListFromAWSResult(resources=dbinstances)
