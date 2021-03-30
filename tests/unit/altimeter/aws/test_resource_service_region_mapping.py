import json
import unittest
import unittest.mock

from altimeter.aws.resource.resource_spec import ScanGranularity
from altimeter.aws.resource_service_region_mapping import (
    build_aws_resource_region_mapping_repo,
    NoRegionsFoundForResource,
)
from altimeter.aws.scan.settings import ALL_RESOURCE_SPEC_CLASSES


class TestAWSResourceRegionMappingRepository(unittest.TestCase):
    def test_with_empty_global_region_whitelist(self):
        sample_data_filepath = "tests/data/aws_service_region_mapping/20210329202700.json"
        with open(sample_data_filepath, "r") as fp:
            region_services_json = json.load(fp)
        with unittest.mock.patch(
            "altimeter.aws.resource_service_region_mapping.get_aws_service_region_mapping_json"
        ) as mock_get_aws_service_region_mapping_json:
            mock_get_aws_service_region_mapping_json.return_value = region_services_json
            mapping_repo = build_aws_resource_region_mapping_repo(
                global_region_whitelist=(),
                preferred_account_scan_regions=("us-east-1",),
                services_regions_json_url="https://mock_url",
            )
            for resource_spec_class in ALL_RESOURCE_SPEC_CLASSES:
                scan_regions = mapping_repo.get_regions(
                    resource_spec_class=resource_spec_class, region_whitelist=()
                )
                if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
                    self.assertEqual(scan_regions, ("us-east-1",))
                else:
                    self.assertGreaterEqual(len(scan_regions), 1)

    def test_with_populated_global_region_whitelist(self):
        sample_data_filepath = "tests/data/aws_service_region_mapping/20210329202700.json"
        with open(sample_data_filepath, "r") as fp:
            region_services_json = json.load(fp)
        with unittest.mock.patch(
            "altimeter.aws.resource_service_region_mapping.get_aws_service_region_mapping_json"
        ) as mock_get_aws_service_region_mapping_json:
            mock_get_aws_service_region_mapping_json.return_value = region_services_json
            mapping_repo = build_aws_resource_region_mapping_repo(
                global_region_whitelist=("us-east-1", "us-east-2", "us-west-1", "us-west-2"),
                preferred_account_scan_regions=("us-east-1",),
                services_regions_json_url="https://mock_url",
            )
            for resource_spec_class in ALL_RESOURCE_SPEC_CLASSES:
                scan_regions = mapping_repo.get_regions(
                    resource_spec_class=resource_spec_class, region_whitelist=()
                )
                if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
                    self.assertEqual(scan_regions, ("us-east-1",))
                else:
                    self.assertGreaterEqual(len(scan_regions), 1)
                    self.assertLessEqual(len(scan_regions), 4)

    def test_with_under_populated_global_region_whitelist(self):
        sample_data_filepath = "tests/data/aws_service_region_mapping/20210329202700.json"
        with open(sample_data_filepath, "r") as fp:
            region_services_json = json.load(fp)
        with unittest.mock.patch(
            "altimeter.aws.resource_service_region_mapping.get_aws_service_region_mapping_json"
        ) as mock_get_aws_service_region_mapping_json:
            mock_get_aws_service_region_mapping_json.return_value = region_services_json
            mapping_repo = build_aws_resource_region_mapping_repo(
                global_region_whitelist=("us-east-2", "us-west-1", "us-west-2"),
                preferred_account_scan_regions=("us-east-1",),
                services_regions_json_url="https://mock_url",
            )
            for resource_spec_class in ALL_RESOURCE_SPEC_CLASSES:
                if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
                    with self.assertRaises(NoRegionsFoundForResource):
                        mapping_repo.get_regions(
                            resource_spec_class=resource_spec_class, region_whitelist=()
                        )
                else:
                    scan_regions = mapping_repo.get_regions(
                        resource_spec_class=resource_spec_class, region_whitelist=()
                    )
                    self.assertGreaterEqual(len(scan_regions), 1)
                    self.assertLessEqual(len(scan_regions), 4)

    def test_with_populated_account_region_whitelist(self):
        sample_data_filepath = "tests/data/aws_service_region_mapping/20210329202700.json"
        with open(sample_data_filepath, "r") as fp:
            region_services_json = json.load(fp)
        with unittest.mock.patch(
            "altimeter.aws.resource_service_region_mapping.get_aws_service_region_mapping_json"
        ) as mock_get_aws_service_region_mapping_json:
            mock_get_aws_service_region_mapping_json.return_value = region_services_json
            mapping_repo = build_aws_resource_region_mapping_repo(
                global_region_whitelist=("us-east-1", "us-east-2", "us-west-1", "us-west-2"),
                preferred_account_scan_regions=("us-east-1",),
                services_regions_json_url="https://mock_url",
            )
            for resource_spec_class in ALL_RESOURCE_SPEC_CLASSES:
                scan_regions = mapping_repo.get_regions(
                    resource_spec_class=resource_spec_class,
                    region_whitelist=("us-east-1", "us-east-2"),
                )
                if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
                    self.assertEqual(scan_regions, ("us-east-1",))
                else:
                    self.assertGreaterEqual(len(scan_regions), 1)
                    self.assertLessEqual(len(scan_regions), 2)

    def test_with_under_populated_account_region_whitelist(self):
        sample_data_filepath = "tests/data/aws_service_region_mapping/20210329202700.json"
        with open(sample_data_filepath, "r") as fp:
            region_services_json = json.load(fp)
        with unittest.mock.patch(
            "altimeter.aws.resource_service_region_mapping.get_aws_service_region_mapping_json"
        ) as mock_get_aws_service_region_mapping_json:
            mock_get_aws_service_region_mapping_json.return_value = region_services_json
            mapping_repo = build_aws_resource_region_mapping_repo(
                global_region_whitelist=("us-east-1", "us-east-2", "us-west-1", "us-west-2"),
                preferred_account_scan_regions=("us-east-1",),
                services_regions_json_url="https://mock_url",
            )
            for resource_spec_class in ALL_RESOURCE_SPEC_CLASSES:
                if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
                    with self.assertRaises(NoRegionsFoundForResource):
                        mapping_repo.get_regions(
                            resource_spec_class=resource_spec_class, region_whitelist=("us-east-2")
                        )
                else:
                    scan_regions = mapping_repo.get_regions(
                        resource_spec_class=resource_spec_class, region_whitelist=("us-east-2")
                    )
                    self.assertEqual(len(scan_regions), 1)
