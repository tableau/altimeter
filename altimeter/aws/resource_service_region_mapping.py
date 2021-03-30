"""Discover service/region availability"""
from collections import defaultdict
import math
import random
from typing import Any, DefaultDict, Dict, List, Tuple, Type

import boto3
import botocore
from pydantic import BaseModel, Field
import requests

from altimeter.aws.scan.settings import ALL_RESOURCE_SPEC_CLASSES
from altimeter.aws.resource.resource_spec import AWSResourceSpec, ScanGranularity
from altimeter.core.log import Logger
from altimeter.aws.log_events import AWSLogEvents


class NoRegionsFoundForResource(Exception):
    """Indicates no regions could be found for a resource"""


class UnsupportedServiceRegionMappingVersion(Exception):
    """Indiciates a service region mapping json artifact is using an unsupported version"""


class AWSResourceRegionMappingRepository(BaseModel):
    """Contains the mapping between AWS resources and regions"""

    boto_service_resource_region_mapping: Dict[str, Dict[str, Tuple[str, ...]]]

    def get_regions(
        self, resource_spec_class: Type[AWSResourceSpec], region_whitelist: Tuple[str, ...]
    ) -> Tuple[str, ...]:
        logger = Logger()
        with logger.bind(
            service_name=resource_spec_class.service_name,
            resource_name=resource_spec_class.type_name,
            region_whitelist=region_whitelist,
            boto3_version=boto3.__version__,
            botocore_version=botocore.__version__,
        ):
            logger.info(event=AWSLogEvents.GetServiceResourceRegionMappingStart)
            service = resource_spec_class.service_name
            resource = resource_spec_class.type_name
            prefiltered_regions = self.boto_service_resource_region_mapping.get(service, {}).get(
                resource, ()
            )
            if region_whitelist:
                regions = tuple(
                    region for region in prefiltered_regions if region in region_whitelist
                )
            else:
                regions = prefiltered_regions
            if not regions:
                raise NoRegionsFoundForResource(
                    f"No regions found for resource {service}/{resource}"
                )
            logger.info(
                event=AWSLogEvents.GetServiceResourceRegionMappingEnd,
                prefiltered_regions=prefiltered_regions,
                regions=regions,
            )
            return regions


def build_aws_resource_region_mapping_repo(
    global_region_whitelist: Tuple[str, ...],
    preferred_account_scan_regions: Tuple[str, ...],
    services_regions_json_url: str,
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...] = ALL_RESOURCE_SPEC_CLASSES,
) -> "AWSResourceRegionMappingRepository":
    """Build mapping representing the region availability of AWS Resources.

    Args:
        global_region_whitelist: if populated this is used as a region whitelist
        preferred_account_scan_regions: regions which should be used for Account granularity resources
        services_regions_json_url: url of aws region/service mapping json
        resource_spec_classes: AWSResourceSpec classes to include in the mapping

    Returns:
        AWSResourceRegionMappingRepository
    """
    logger = Logger()
    services = tuple(
        resource_spec_class.service_name for resource_spec_class in resource_spec_classes
    )
    aws_service_region_mapping = {}
    try:
        aws_service_region_mapping = get_aws_service_region_mapping(
            services=services, services_regions_json_url=services_regions_json_url,
        )
    except Exception as ex:
        logger.warn(
            event=AWSLogEvents.GetServiceResourceRegionMappingWarning,
            services_regions_json_url=services_regions_json_url,
            msg=str(ex),
        )
    boto_service_region_mapping = get_boto_service_region_mapping(services=services)
    boto_service_resource_region_mapping: DefaultDict[
        str, Dict[str, Tuple[str, ...]]
    ] = defaultdict(dict)
    for resource_spec_class in resource_spec_classes:
        resource_name = resource_spec_class.type_name
        service_name = resource_spec_class.service_name
        candidate_regions = boto_service_region_mapping.get(service_name, ())
        if "aws-global" in candidate_regions:
            if resource_spec_class.scan_granularity != ScanGranularity.ACCOUNT:
                raise Exception(
                    f"BUG: botocore service/region mapping contains {resource_spec_class} "
                    f"region aws-global but class is marked {resource_spec_class.scan_granularity} granularity"
                )
            candidate_regions = preferred_account_scan_regions
        else:
            if aws_service_region_mapping:
                # check against the aws_service_region_mapping, warn if any missing regions are found in the
                # botocore mapping
                aws_regions = frozenset(aws_service_region_mapping.get(service_name, []))
                boto_regions = frozenset(boto_service_region_mapping.get(service_name, []))
                boto_missing = aws_regions - boto_regions
                if boto_missing:
                    logger.warn(
                        event=AWSLogEvents.GetServiceResourceRegionMappingDiscrepancy,
                        msg=(
                            f"{service_name} botocore mappings appear to be missing region(s): "
                            f"{', '.join(boto_missing)}. You likely need to update the botocore version in Altimeter "
                            "and redeploy otherwise this service/region will not be scanned."
                        ),
                        boto3_version=boto3.__version__,
                        botocore_version=botocore.__version__,
                    )
        if resource_spec_class.region_whitelist:
            candidate_regions = tuple(
                region
                for region in resource_spec_class.region_whitelist
                if region in candidate_regions
            )
        if global_region_whitelist:
            candidate_regions = tuple(
                region for region in candidate_regions if region in global_region_whitelist
            )
        if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
            if candidate_regions:
                candidate_regions = tuple(
                    region
                    for region in candidate_regions
                    if region in preferred_account_scan_regions
                )
                if candidate_regions:
                    candidate_regions = (random.choice(candidate_regions),)
        boto_service_resource_region_mapping[service_name][resource_name] = candidate_regions
    return AWSResourceRegionMappingRepository(
        boto_service_resource_region_mapping=boto_service_resource_region_mapping,
    )


def get_boto_service_region_mapping(services: Tuple[str, ...]) -> Dict[str, Tuple[str, ...]]:
    """Return a mapping of service names to supported regions for the given services using boto"""
    service_region_mapping: Dict[str, Tuple[str, ...]] = {}
    session = boto3.Session()
    for service in services:
        service_region_mapping[service] = tuple(
            session.get_available_regions(
                service_name=service, partition_name="aws", allow_non_regional=True
            )
            + session.get_available_regions(
                service_name=service, partition_name="aws-cn", allow_non_regional=True
            )
            + session.get_available_regions(
                service_name=service, partition_name="aws-us-gov", allow_non_regional=True
            )
        )
    return service_region_mapping


def get_aws_service_region_mapping_json(services_regions_json_url: str) -> Dict[str, Any]:
    """Read AWS service/region mapping json and return as a dict"""
    region_services_resp = requests.get(services_regions_json_url)
    return region_services_resp.json()


def get_aws_service_region_mapping(
    services: Tuple[str, ...], services_regions_json_url: str
) -> Dict[str, Tuple[str, ...]]:
    """Return a mapping of service names to supported regions for the given services using advertised json"""

    class RawServiceRegionDict(BaseModel):
        class RawServiceRegionDictMetadata(BaseModel):
            version: float = Field(alias="format:version")

        class RawService(BaseModel):
            class RawServiceAttributes(BaseModel):
                region: str = Field(alias="aws:region")

            attributes: RawServiceAttributes
            id: str

        metadata: RawServiceRegionDictMetadata
        services: List[RawService] = Field(alias="prices")

    region_services_json = get_aws_service_region_mapping_json(
        services_regions_json_url=services_regions_json_url
    )
    raw_service_region_mapping = RawServiceRegionDict(**region_services_json)
    expected_major_version: int = 1
    major_version = math.floor(raw_service_region_mapping.metadata.version)
    if major_version != expected_major_version:
        raise UnsupportedServiceRegionMappingVersion(
            f"Expected metadata -> format:version major_version {expected_major_version}, got {major_version}"
        )
    service_region_mapping: DefaultDict[str, Tuple[str, ...]] = defaultdict(tuple)
    for service in raw_service_region_mapping.services:
        service_name, service_region = service.id.split(":")
        if service_name in services:
            service_region_mapping[service_name] += (service_region,)
    return service_region_mapping
