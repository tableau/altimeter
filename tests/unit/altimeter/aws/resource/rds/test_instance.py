from unittest import TestCase
from unittest.mock import patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_rds2

from altimeter.aws.resource.rds.instance import RDSInstanceResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestRDSInstanceResourceSpec(TestCase):
    @mock_rds2
    def test_disappearing_instance_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        instance_name = "foo"

        session = boto3.Session()
        client = session.client("rds", region_name=region_name)

        client.create_db_instance(
            DBInstanceIdentifier=instance_name,
            Engine="postgres",
            DBName=instance_name,
            DBInstanceClass="db.m1.small",
            MasterUsername="root",
            MasterUserPassword="hunter2",
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.rds.instance.RDSInstanceResourceSpec.get_instance_tags"
        ) as mock_get_instance_tags:
            with patch(
                "altimeter.aws.resource.rds.instance.RDSInstanceResourceSpec.set_automated_backups"
            ) as mock_set_automated_backups:
                mock_set_automated_backups.return_value = None
                mock_get_instance_tags.side_effect = ClientError(
                    operation_name="ListTagsForResource",
                    error_response={
                        "Error": {
                            "Code": "DBInstanceNotFound",
                            "Message": f"Could not find a DB Instance matching the resource name: '{instance_name}'",
                        }
                    },
                )
                resources = RDSInstanceResourceSpec.scan(scan_accessor=scan_accessor)
                self.assertEqual(resources, [])
