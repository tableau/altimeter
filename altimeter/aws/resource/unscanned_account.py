"""Resource representing an unscanned AWS Account"""
from typing import List, Type
import uuid

from botocore.client import BaseClient

from altimeter.core.graph.links import LinkCollection, SimpleLink
from altimeter.core.graph.schema import Schema
from altimeter.aws.resource.resource_spec import ScanGranularity, ListFromAWSResult, AWSResourceSpec
from altimeter.core.resource.resource import Resource
from altimeter.aws.scan.aws_accessor import AWSAccessor


class UnscannedAccountResourceSpec(AWSResourceSpec):
    """Resource representing an unscanned AWS Account"""

    type_name = "unscanned-account"
    service_name = "null"
    scan_granularity = ScanGranularity.ACCOUNT
    schema = Schema()

    @classmethod
    def create_resource(
        cls: Type["UnscannedAccountResourceSpec"], account_id: str, errors: List[str]
    ) -> Resource:
        simple_links: List[SimpleLink] = []
        simple_links.append(SimpleLink(pred="account_id", obj=account_id))
        if errors:
            error = "\n".join(errors)
            simple_links.append(SimpleLink(pred="error", obj=f"{error} - {uuid.uuid4()}"))
        return Resource(
            resource_id=cls.generate_arn(resource_id=account_id),
            type=cls.get_full_type_name(),
            link_collection=LinkCollection(simple_links=simple_links),
        )

    @classmethod
    def get_full_type_name(cls: Type["UnscannedAccountResourceSpec"]) -> str:
        return f"{cls.provider_name}:{cls.type_name}"

    @classmethod
    def list_from_aws(
        cls: Type["UnscannedAccountResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """List resources from AWS using client."""

    @classmethod
    def generate_arn(
        cls: Type["UnscannedAccountResourceSpec"],
        resource_id: str,
        account_id: str = "",
        region: str = "",
    ) -> str:
        """Generate an ARN for this resource"""
        return f"arn:aws::::account/{resource_id}"

    @classmethod
    def scan(
        cls: Type["UnscannedAccountResourceSpec"], scan_accessor: AWSAccessor
    ) -> List[Resource]:
        raise NotImplementedError(f"{cls.__name__} is not a scannable ResourceSpec class.")
