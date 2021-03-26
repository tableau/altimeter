"""Discover service/region availability"""
from collections import defaultdict
import math
import random
from typing import Any, DefaultDict, Dict, List, Mapping, Optional, Tuple, Type
from types import MappingProxyType

import boto3
from pydantic import BaseModel, Field
import requests

from altimeter.aws.scan.settings import ALL_RESOURCE_SPEC_CLASSES
from altimeter.aws.resource.resource_spec import AWSResourceSpec, ScanGranularity
from altimeter.core.log import Logger
from altimeter.aws.log_events import AWSLogEvents


class NoRegionsFoundForResource(Exception):
    """Indicates no regions could be found for a resource"""


class AWSResourceRegionMappingRepository(BaseModel):
    """Contains the mappings between AWS resources and regions"""

    boto_service_resource_region_mappings: Mapping[str, Mapping[str, Tuple[str, ...]]]

    def get_regions(
        self, resource_spec_class: Type[AWSResourceSpec], region_whitelist: Tuple[str, ...]
    ) -> Tuple[str, ...]:
        logger = Logger()
        with logger.bind(resource_spec_class=resource_spec_class):
            logger.info(event=AWSLogEvents.GetServiceResourceRegionMappingStart)
            service = resource_spec_class.service_name
            resource = resource_spec_class.type_name
            prefiltered_regions = self.boto_service_resource_region_mappings.get(service, {}).get(
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
            logger.info(event=AWSLogEvents.GetServiceResourceRegionMappingEnd)
            return regions


def build_aws_resource_region_mapping_repo(
    services_regions_json_url: str,
    global_region_whitelist: Tuple[str, ...],
    preferred_account_scan_regions: Tuple[str, ...],
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...] = ALL_RESOURCE_SPEC_CLASSES,
) -> "AWSResourceRegionMappingRepository":
    """Build mappings representing the region availability of AWS Resources.

    Args:
        services_regions_json_url: url to AWS advertised service/region mapping. This is used
            to check for any updates not present in the boto3 mappings. If any are found a warning
            level log entry is emitted.
        global_region_whitelist: if populated this is used as a region whitelist
        preferred_account_scan_regions: regions which should be used for Account granularity resources
        resource_spec_classes: AWSResourceSpec classes to include in the mappings

    Returns:
        AWSResourceRegionMappingRepository
    """
    logger = Logger()
    services = tuple(
        resource_spec_class.service_name for resource_spec_class in resource_spec_classes
    )
    boto_service_region_mappings = get_boto_service_region_mapping(services=services)
    # check if for any service there are any regions advertsied in the aws json mappings which are not in the
    # boto mappings. If so, emit a warn level log event
    try:
        aws_service_region_mappings = get_aws_service_region_mappings(
            services=services, services_regions_json_url=services_regions_json_url
        )
        for check_service_name in boto_service_region_mappings.keys():
            if check_service_name in aws_service_region_mappings:
                boto_service_regions = frozenset(boto_service_region_mappings[check_service_name])
                if "aws-global" in boto_service_regions:
                    continue
                aws_service_regions = frozenset(aws_service_region_mappings[check_service_name])
                service_regions_in_aws_not_boto = aws_service_regions - boto_service_regions
                if service_regions_in_aws_not_boto:
                    logger.warning(
                        event=AWSLogEvents.GetServiceResourceRegionMappingDiscrepancy,
                        service=check_service_name,
                        regions=service_regions_in_aws_not_boto,
                        msg="Regions found in aws JSON but not in boto. You likely need to upgrade boto.",
                    )
    except Exception as ex:
        logger.warning(
            event=AWSLogEvents.GetServiceResourceRegionMappingAWSJSONError, error=str(ex)
        )

    raw_boto_service_resource_region_mappings: DefaultDict[
        str, Dict[str, Tuple[str, ...]]
    ] = defaultdict(dict)
    for resource_spec_class in resource_spec_classes:
        resource_name = resource_spec_class.type_name
        service_name = resource_spec_class.service_name
        candidate_regions = boto_service_region_mappings.get(service_name, ())
        if "aws-global" in candidate_regions:
            if resource_spec_class.scan_granularity != ScanGranularity.ACCOUNT:
                raise Exception(
                    f"BUG: boto mappings contain {resource_spec_class} "
                    f"region aws-global but class is marked {resource_spec_class.scan_granularity} granularity"
                )
            candidate_regions = preferred_account_scan_regions
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
        raw_boto_service_resource_region_mappings[service_name][resource_name] = candidate_regions
    less_mutable_boto_service_resource_region_mappings: Dict[
        str, Mapping[str, Tuple[str, ...]]
    ] = {}
    for service_name, resource_region_mapping in raw_boto_service_resource_region_mappings.items():
        less_mutable_boto_service_resource_region_mappings[service_name] = MappingProxyType(
            resource_region_mapping
        )
    boto_service_resource_region_mappings = MappingProxyType(
        less_mutable_boto_service_resource_region_mappings
    )

    return AWSResourceRegionMappingRepository(
        boto_service_resource_region_mappings=boto_service_resource_region_mappings,
    )


def get_boto_service_region_mapping(services: Tuple[str, ...]) -> Mapping[str, Tuple[str, ...]]:
    """Return a mapping of service names to supported regions for the given services using boto"""
    service_region_mapping: Dict[str, Tuple[str, ...]] = {}
    session = boto3.Session()
    for service in services:
        aws_partition_regions = session.get_available_regions(
            service_name=service, partition_name="aws", allow_non_regional=True
        )
        us_gov_partition_regions = session.get_available_regions(
            service_name=service, partition_name="aws-us-gov", allow_non_regional=True
        )
        cn_partition_regions = session.get_available_regions(
            service_name=service, partition_name="aws-cn", allow_non_regional=True
        )
        service_region_mapping[service] = tuple(
            aws_partition_regions + us_gov_partition_regions + cn_partition_regions
        )
    return MappingProxyType(service_region_mapping)


class UnsupportedServiceRegionMappingVersion(Exception):
    """Indiciates a service region mapping json artifact is using an unsupported version"""


def get_aws_service_region_mappings_json(services_regions_json_url: str) -> Mapping[str, Any]:
    """Read AWS service/region mappings json and return as a MappingProxyType"""
    region_services_resp = requests.get(services_regions_json_url)
    region_services_json = region_services_resp.json()
    return MappingProxyType(region_services_json)


def get_aws_service_region_mappings(
    services: Tuple[str, ...], services_regions_json_url: str
) -> Mapping[str, Tuple[str, ...]]:
    """Return a mapping of service names to supported regions for the given services using advertised json"""

    class RawServiceRegionMapping(BaseModel):
        class RawServiceRegionMappingMetadata(BaseModel):
            version: float = Field(alias="format:version")

        class RawService(BaseModel):
            class RawServiceAttributes(BaseModel):
                region: str = Field(alias="aws:region")

            attributes: RawServiceAttributes
            id: str

        metadata: RawServiceRegionMappingMetadata
        services: List[RawService] = Field(alias="prices")

    region_services_json = get_aws_service_region_mappings_json(
        services_regions_json_url=services_regions_json_url
    )

    raw_service_region_mapping = RawServiceRegionMapping(**region_services_json)
    expected_major_version: int = 1
    major_version = math.floor(raw_service_region_mapping.metadata.version)
    if major_version != expected_major_version:
        raise UnsupportedServiceRegionMappingVersion(
            f"Expected metadata -> format:version major_version {expected_major_version}, got {major_version}"
        )
    service_region_mappings: DefaultDict[str, Tuple[str, ...]] = defaultdict(tuple)
    for service in raw_service_region_mapping.services:
        service_name, service_region = service.id.split(":")
        if service_name in services:
            service_region_mappings[service_name] += (service_region,)
    return MappingProxyType(service_region_mappings)
