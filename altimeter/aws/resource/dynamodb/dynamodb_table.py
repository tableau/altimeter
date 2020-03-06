"""Resource for IAM Policies"""
from typing import Any, Type, List, Dict

from botocore.client import BaseClient

from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.dict_field import AnonymousDictField
from altimeter.core.graph.schema import Schema
from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.dynamodb import DynamoDBResourceSpec


class DynamoDbTableResourceSpec(DynamoDBResourceSpec):
    """Resource for DynamoDB Tables"""

    type_name = "table"
    schema = Schema(
        ScalarField("TableName", "name"),
        ScalarField("TableStatus"),
        ScalarField("CreationDateTime"),
        ScalarField("TableSizeBytes"),
        ScalarField("ItemCount"),
        ScalarField("TableId"),
        AnonymousDictField(
            "ProvisionedThroughput", ScalarField("NumberOfDecreasesToday", optional=True)
        ),
        AnonymousDictField("ProvisionedThroughput", ScalarField("ReadCapacityUnits")),
        AnonymousDictField("ProvisionedThroughput", ScalarField("WriteCapacityUnits")),
        AnonymousDictField("BillingModeSummary", ScalarField("BillingMode"), optional=True),
        AnonymousDictField(
            "BillingModeSummary",
            ScalarField("LastUpdateToPayPerRequestDateTime", optional=True),
            optional=True,
        ),
        AnonymousDictField("ContinuousBackupsDescription", ScalarField("ContinuousBackupsStatus")),
        AnonymousDictField(
            "ContinuousBackupsDescription",
            AnonymousDictField(
                "PointInTimeRecoveryDescription",
                ScalarField("LatestRestorableDateTime", optional=True),
            ),
        ),
        AnonymousDictField(
            "ContinuousBackupsDescription",
            AnonymousDictField(
                "PointInTimeRecoveryDescription",
                ScalarField("EarliestRestorableDateTime", optional=True),
            ),
        ),
        AnonymousDictField(
            "ContinuousBackupsDescription",
            AnonymousDictField(
                "PointInTimeRecoveryDescription",
                ScalarField("PointInTimeRecoveryStatus", optional=True),
            ),
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["DynamoDbTableResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        table_names: List[str] = []
        tables: Dict[str, Dict[str, Any]] = {}
        paginator = client.get_paginator("list_tables")

        for resp in paginator.paginate():
            table_names.extend(resp.get("TableNames", []))

        for table_name in table_names:
            table_data = get_table_data(client=client, table_name=table_name)
            continuous_backup_data = get_continuous_backup_table_data(
                client=client, table_name=table_name
            )
            table_data.update(continuous_backup_data)
            resource_arn = table_data["TableArn"]
            tables[resource_arn] = table_data
        return ListFromAWSResult(resources=tables)


def get_table_data(client: BaseClient, table_name: str) -> Dict:
    """Retrieve detailed properties of DynamoDB table"""
    table_resp = client.describe_table(TableName=table_name)
    table_data = table_resp.get("Table", None)
    return table_data


def get_continuous_backup_table_data(client: BaseClient, table_name: str) -> Dict:
    """Retrieve detailed properties of DynamoDB table"""
    resp = client.describe_continuous_backups(TableName=table_name)
    backup_data = {"ContinuousBackupsDescription": resp.get("ContinuousBackupsDescription", None)}
    return backup_data
