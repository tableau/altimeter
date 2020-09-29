"""AWSResourceSpec is a subclass of ResourceSpec which is used to define
ResourceSpecs for AWS resources"""
import abc
import inspect
from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, Any, List, Tuple, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.core.graph.exceptions import SchemaParseException
from altimeter.core.graph.link.links import ResourceLinkLink
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec

AWS_API_IGNORE_ERRORS = frozenset(
    ("NotSignedUp", "OptInRequired", "SubscriptionRequiredException", "InvalidAction")
)


class ScanGranularity(Enum):
    """ScanGranularities are attached to AWSResourceSpecs and define how resources are scanned."""

    REGION = auto()  # scanned in every region
    ACCOUNT = auto()  # scanned in only one region


@dataclass(frozen=True)
class ListFromAWSResult:
    """Result of a list_from_aws call. Contains a list of resources represented as a dict
    of arns to resource details

    Args:
        resources: Dict of resource ids to resource dicts
    """

    resources: Dict[str, Dict[str, Any]]


class AWSResourceSpec(ResourceSpec):
    """AWSResourceSpec is a subclass of ResourceSpec which is used to define
    ResourceSpecs for AWS resources"""

    provider_name: str = "aws"
    service_name: str = ""
    scan_granularity: ScanGranularity = ScanGranularity.REGION
    region_whitelist: Tuple[str, ...] = ()
    parallel_scan: bool = False

    def __init_subclass__(cls: Type["AWSResourceSpec"], **kwargs: Any) -> None:
        if not inspect.isabstract(cls):
            for required in ("service_name",):
                if not getattr(cls, required):
                    raise TypeError(
                        f"Can not instantiate {cls.__name__} without {required} attribute."
                    )
        return super().__init_subclass__(**kwargs)

    @classmethod
    def get_full_type_name(cls: Type["AWSResourceSpec"]) -> str:
        """Get the fully qualified type name for this class, generally something like
        aws:ec2:instance, aws:iam:role, etc.

        Returns:
            string of full type name, generally something like "aws:ec2:instance"
        """
        return f"{cls.provider_name}:{cls.service_name}:{cls.type_name}"

    @classmethod
    def get_client_name(cls: Type["AWSResourceSpec"]) -> str:
        """Get the boto3 client name to be used for scanning resources of this type.
        Generally this is the same as cls.service_name but in some cases it is not.

        Returns:
             string of boto3 client name for cls.service
        """
        return cls.service_name

    @classmethod
    def generate_id(
        cls: Type["AWSResourceSpec"], short_resource_id: str, context: Dict[str, Any]
    ) -> str:
        """Generate a full id (arn) given a short resource id.

        Args:
            short_resource_id: last portion of an aws arn - e.g. i-1234, ami-abcd, etc.
            context: dict containing account_id, region

        Returns:
            string containing resource arn.
        """

        return cls.generate_arn(
            account_id=context.get("account_id", ""),
            region=context.get("region", ""),
            resource_id=short_resource_id,
        )

    @classmethod
    def generate_arn(
        cls: Type["AWSResourceSpec"], resource_id: str, account_id: str = "", region: str = "",
    ) -> str:
        """Generate an ARN for this resource

        Args:
            account_id: resource account id
            region: resource region
            resource_id: resource id

        Returns:
            string containing resource arn.
        """
        return (
            ":".join(
                ("arn", cls.provider_name, cls.service_name, region, account_id, cls.type_name)
            )
            + f"/{resource_id}"
        )

    @classmethod
    def skip_resource_scan(
        cls: Type["AWSResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> bool:
        """Return a bool indicating whether this resource class scan should be skipped.
        Args:
            client: boto3 client
            account_id: account id
            region: aws region

        Returns:
            True if this resource should be skipped.
        """
        return False

    @classmethod
    @abc.abstractmethod
    def list_from_aws(
        cls: Type["AWSResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a ListFromAWSResult object by calling the appropriate
        AWS API calls to list/describe the resource represented by this class.

        Args:
            client: boto3 Client
            account_id: aws account id
            region: aws region

        Returns:
            ListFromAWSResult object
        """

    @classmethod
    def scan(cls: Type["AWSResourceSpec"], scan_accessor: AWSAccessor) -> List[Resource]:
        """Scan this ResourceSpec

       Args:
           scan_accessor: AWSAccessor object to use for api access

        Returns:
            List of Resource objects
        """
        context = {"account_id": scan_accessor.account_id, "region": scan_accessor.region}
        list_from_aws_result = cls._list_from_aws(scan_accessor)
        resources = cls._list_from_aws_result_to_resources(
            list_from_aws_result=list_from_aws_result, context=context
        )
        return resources

    @classmethod
    def _list_from_aws(
        cls: Type["AWSResourceSpec"], scan_accessor: AWSAccessor
    ) -> ListFromAWSResult:
        try:
            resource_client = scan_accessor.client(cls.get_client_name())
            if cls.skip_resource_scan(
                client=resource_client,
                account_id=scan_accessor.account_id,
                region=scan_accessor.region,
            ):
                return ListFromAWSResult(resources={})
            return cls.list_from_aws(
                resource_client, scan_accessor.account_id, scan_accessor.region
            )
        except ClientError as c_e:
            response_error = getattr(c_e, "response", {}).get("Error", {})
            error_code = response_error.get("Code", "")
            if error_code not in AWS_API_IGNORE_ERRORS:
                raise c_e
            return ListFromAWSResult(resources={})

    @classmethod
    def _list_from_aws_result_to_resources(
        cls: Type["AWSResourceSpec"],
        list_from_aws_result: ListFromAWSResult,
        context: Dict[str, str],
    ) -> List[Resource]:
        resources: List[Resource] = []
        for arn, resource_dict in list_from_aws_result.resources.items():
            try:
                links = cls.schema.parse(resource_dict, context)
            except Exception as ex:
                raise SchemaParseException(
                    (
                        f"Error parsing {cls.__name__} : "
                        f"{arn}:\n\nData: {resource_dict}\n\nError: {ex}"
                    )
                )
            resource_account_id = arn.split(":")[4]
            if resource_account_id:
                if resource_account_id != "aws":
                    account_link = ResourceLinkLink(
                        pred="account", obj=f"arn:aws::::account/{resource_account_id}"
                    )
                    links.append(account_link)
                    resource_region_name = arn.split(":")[3]
                    if resource_region_name:
                        region_link = ResourceLinkLink(
                            pred="region",
                            obj=f"arn:aws:::{resource_account_id}:region/{resource_region_name}",
                        )
                        links.append(region_link)
            resource = Resource(resource_id=arn, type_name=cls.get_full_type_name(), links=links)
            resources.append(resource)
        return resources
