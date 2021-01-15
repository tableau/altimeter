from unittest import TestCase
from unittest.mock import patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_dynamodb2

from altimeter.aws.resource.dynamodb.dynamodb_table import DynamoDbTableResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestDynamoDbTableResourceSpec(TestCase):
    @mock_dynamodb2
    def test_disappearing_table_get_table_data_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        table_name = "foo"

        session = boto3.Session()
        client = session.client("dynamodb", region_name=region_name)

        client.create_table(
            AttributeDefinitions=[{"AttributeName": "string", "AttributeType": "S",},],
            KeySchema=[{"AttributeName": "string", "KeyType": "HASH",},],
            TableName=table_name,
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.dynamodb.dynamodb_table.get_table_data"
        ) as mock_get_table_data:
            mock_get_table_data.side_effect = ClientError(
                operation_name="DescribeTable",
                error_response={
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": f"Requested resource not found: Table: {table_name} not found",
                    }
                },
            )
            resources = DynamoDbTableResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])

    @mock_dynamodb2
    def test_disappearing_table_get_continuous_backup_table_data_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        table_name = "foo"

        session = boto3.Session()
        client = session.client("dynamodb", region_name=region_name)

        client.create_table(
            AttributeDefinitions=[{"AttributeName": "string", "AttributeType": "S",},],
            KeySchema=[{"AttributeName": "string", "KeyType": "HASH",},],
            TableName=table_name,
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.dynamodb.dynamodb_table.get_continuous_backup_table_data"
        ) as mock_get_continuous_backup_table_data:
            mock_get_continuous_backup_table_data.side_effect = ClientError(
                operation_name="DescribeContinuousBackups",
                error_response={
                    "Error": {
                        "Code": "TableNotFoundException",
                        "Message": f"Table not found: {table_name}",
                    }
                },
            )
            resources = DynamoDbTableResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
