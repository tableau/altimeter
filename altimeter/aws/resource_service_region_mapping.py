"""Discover service/region availability"""
from collections import defaultdict
import math
import random
from typing import Any, DefaultDict, Dict, List, Mapping, Tuple, Type
from types import MappingProxyType

import boto3
from pydantic import BaseModel, Field
import requests

from altimeter.aws.scan.settings import ALL_RESOURCE_SPEC_CLASSES
from altimeter.aws.resource.resource_spec import AWSResourceSpec, ScanGranularity
from altimeter.core.log import Logger
from altimeter.aws.log_events import AWSLogEvents


class UnsupportedServiceRegionMappingVersion(Exception):
    """Indicates a service region mapping json artifact is using an unsupported version"""


class NoRegionsFoundForResource(Exception):
    """Indicates no regions could be found for a resource"""


class AWSResourceRegionMappingRepository(BaseModel):
    """Contains the mappings between AWS resources and regions"""

    aws_service_resource_region_mappings: Mapping[str, Mapping[str, Tuple[str, ...]]]
    boto_service_resource_region_mappings: Mapping[str, Mapping[str, Tuple[str, ...]]]

    def get_regions(
        self, resource_spec_class: Type[AWSResourceSpec], region_whitelist: Tuple[str, ...]
    ) -> Tuple[str, ...]:
        logger = Logger()
        with logger.bind(resource_spec_class=resource_spec_class):
            logger.info(event=AWSLogEvents.GetServiceResourceRegionMappingStart)
            service = resource_spec_class.service_name
            resource = resource_spec_class.type_name
            prefiltered_regions = self.aws_service_resource_region_mappings.get(service, {}).get(
                resource,
                self.boto_service_resource_region_mappings.get(service, {}).get(resource, ()),
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
        services_regions_json_url: url of the AWS advertised service/region mapping
        global_region_whitelist: if populated this is used as a region whitelist
        preferred_account_scan_regions: regions which should be used for Account granularity resources
        resource_spec_classes: AWSResourceSpec classes to include in the mappings

    Returns:
        AWSResourceRegionMappingRepository
    """
    services = tuple(
        resource_spec_class.service_name for resource_spec_class in resource_spec_classes
    )
    boto_service_region_mappings = get_boto_service_region_mapping(services=services)
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
        if resource_spec_class.region_blacklist:
            candidate_regions = tuple(
                region
                for region in resource_spec_class.region_whitelist
                if region not in candidate_regions
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

    aws_service_resource_region_mappings: Mapping[
        str, Mapping[str, Tuple[str, ...]]
    ] = MappingProxyType({})

    return AWSResourceRegionMappingRepository(
        boto_service_resource_region_mappings=boto_service_resource_region_mappings,
        aws_service_resource_region_mappings=aws_service_resource_region_mappings,
    )


#
#        raw_aws_service_region_mappings: Dict[str, List[str]] = {}
#
#        logger = Logger()
#        with logger.bind(services_regions_json_url=services_regions_json_url):
#            logger.info(event=LogEvent.ServiceRegionDiscoveryStart)
#            # first try discovering from the advertised regions/services json mapping
#            try:
#                raw_aws_service_region_mappings = get_aws_service_region_mapping(
#                    services=services, services_regions_json_url=services_regions_json_url,
#                )
#            except Exception as ex:
#                # then fall back to using boto
#                logger.warning(event=LogEvent.ServiceRegionDiscoveryError, error=str(ex))
#            finally:
#                logger.info(event=LogEvent.ServiceRegionDiscoveryEnd)
#
#        # filter using global_region_whitelist
#        global_region_whitelist_filtered_aws_service_region_mappings = {
#            service: [region for region in regions if region in global_region_whitelist]
#            for service, regions in raw_aws_service_region_mappings.items()
#        }
#        global_region_whitelist_filtered_boto_service_region_mappings = {
#            service: [region for region in regions if region in global_region_whitelist]
#            for service, regions in raw_boto_service_region_mappings.items()
#        }
#        # this is all wrong. this stuff should be by resource not service...
#        # filter Account ScanGranularity resources using preferred_account_scan_regions
#        for resource_spec_class in resource_spec_classes:
#            if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
#                service = resource_spec_class.service_name
#                resource = resource_spec_class.type_name
#                if service in global_region_whitelist_filtered_aws_service_region_mappings:
#                    service_regions = global_region_whitelist_filtered_aws_service_region_mappings[
#                        service
#                    ]
#                    candidate_regions = [
#                        region
#                        for region in service_regions
#                        if region in preferred_account_scan_regions
#                    ]
#                    if not candidate_regions:
#                        raise NoRegionsFoundForResource(
#                            f"No regions found for resource {service}/{resource}"
#                        )
#                    random_region = random.choice(candidate_regions)
#                    global_region_whitelist_filtered_aws_service_region_mappings[service] = (
#                        random_region,
#                    )
#                if service in global_region_whitelist_filtered_boto_service_region_mappings:
#                    service_regions = global_region_whitelist_filtered_boto_service_region_mappings[
#                        service
#                    ]
#                    candidate_regions = [
#                        region
#                        for region in service_regions
#                        if region in preferred_account_scan_regions
#                    ]
#                    if not candidate_regions:
#                        raise NoRegionsFoundForResource(
#                            f"No regions found for resource {service}/{resource}"
#                        )
#                    random_region = random.choice(candidate_regions)
#                    global_region_whitelist_filtered_boto_service_region_mappings[service] = (
#                        random_region,
#                    )
#
#        # make immutable
#        aws_service_region_mappings = MappingProxyType(
#            {
#                service: tuple(regions)
#                for service, regions in global_region_whitelist_filtered_aws_service_region_mappings.items()
#            }
#        )
#        boto_service_region_mappings = MappingProxyType(
#            {
#                service: tuple(regions)
#                for service, regions in global_region_whitelist_filtered_boto_service_region_mappings.items()
#            }
#        )


def get_boto_service_region_mapping(services: Tuple[str, ...]) -> Mapping[str, Tuple[str, ...]]:
    """Return a mapping of service names to supported regions for the given services using boto"""
    service_region_mapping: Dict[str, Tuple[str, ...]] = {}
    session = boto3.Session()
    for service in services:
        service_region_mapping[service] = tuple(
            session.get_available_regions(service_name=service, allow_non_regional=True)
        )
    return MappingProxyType(service_region_mapping)


# def get_aws_service_region_mapping_json(services_regions_json_url: str) -> Dict[str, Any]:
#    """Fetch and return the AWS advertised service region mapping."""
#    region_services_resp = requests.get(services_regions_json_url, timeout=30)
#    region_services_resp.raise_for_status()
#    return region_services_resp.json()
#
#
# def get_aws_service_region_mapping(
#    services: Tuple[str, ...], services_regions_json_url: str
# ) -> Dict[str, List[str, ...]]:
#    """Return a mapping of service names to supported regions for the given services using the AWS provided service
#    region mapping json"""
#
#    class RawServiceRegionMapping(BaseModel):
#        class RawServiceRegionMappingMetadata(BaseModel):
#            version: float = Field(alias="format:version")
#
#        class RawService(BaseModel):
#            class RawServiceAttributes(BaseModel):
#                region: str = Field(alias="aws:region")
#
#            attributes: RawServiceAttributes
#            id: str
#
#        metadata: RawServiceRegionMappingMetadata
#        services: List[RawService] = Field(alias="prices")
#
#    region_services_json = get_aws_service_region_mapping_json(
#        services_regions_json_url=services_regions_json_url
#    )
#    raw_service_region_mapping = RawServiceRegionMapping(**region_services_json)
#    expected_major_version: int = 1
#    major_version = math.floor(raw_service_region_mapping.metadata.version)
#    if major_version != expected_major_version:
#        raise UnsupportedServiceRegionMappingVersion(
#            f"Expected metadata -> format:version major_version {expected_major_version}, got {major_version}"
#        )
#    service_region_mapping: DefaultDict[str, List[str]] = defaultdict(list)
#    for service in raw_service_region_mapping.services:
#        service_name, service_region = service.id.split(":")
#        if service_name in services:
#            service_region_mapping[service_name] += (service_region,)
#    return service_region_mapping
#
