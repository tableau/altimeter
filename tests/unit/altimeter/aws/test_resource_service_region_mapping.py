import json
import unittest
import unittest.mock

from altimeter.aws.resource_service_region_mapping import AWSResourceRegionMappingRepository


class TestAWSResourceRegionMappingRepository(unittest.TestCase):
    def test_with_valid_json_data(self):
        pass


# class TestGetAWSServiceRegionMapping(unittest.TestCase):
#    def test_with_valid_data(self):
#        services = ("ec2", "elb", "lambda")
#        sample_data_filepath = "tests/data/services_regions/sample.json"
#        with open(sample_data_filepath, "r") as fp:
#            region_services_json = json.load(fp)
#        expected_aws_service_region_mapping = {
#            "ec2": (
#                "af-south-1",
#                "ap-northeast-3",
#                "ap-south-1",
#                "eu-north-1",
#                "eu-west-2",
#                "sa-east-1",
#                "us-gov-west-1",
#                "ap-northeast-2",
#                "ap-southeast-1",
#                "cn-north-1",
#                "eu-south-1",
#                "us-east-1",
#                "us-east-2",
#                "us-gov-east-1",
#                "us-west-2",
#                "ca-central-1",
#                "cn-northwest-1",
#                "eu-central-1",
#                "us-west-1",
#            ),
#            "elb": (
#                "ap-northeast-1",
#                "ap-northeast-3",
#                "ap-south-1",
#                "cn-north-1",
#                "eu-south-1",
#                "eu-west-2",
#                "me-south-1",
#                "us-east-2",
#                "us-west-2",
#                "af-south-1",
#                "ap-northeast-2",
#                "cn-northwest-1",
#                "eu-central-1",
#                "eu-west-1",
#                "eu-west-3",
#                "us-east-1",
#                "us-gov-west-1",
#                "ap-southeast-1",
#                "ap-southeast-2",
#                "ca-central-1",
#                "us-west-1",
#            ),
#            "lambda": (
#                "ap-east-1",
#                "ap-northeast-2",
#                "ap-south-1",
#                "ap-southeast-1",
#                "ap-southeast-2",
#                "me-south-1",
#                "us-east-1",
#                "us-east-2",
#                "us-gov-west-1",
#                "ap-northeast-1",
#                "ca-central-1",
#                "eu-north-1",
#                "eu-south-1",
#                "eu-west-1",
#                "eu-west-3",
#                "sa-east-1",
#                "us-west-2",
#                "af-south-1",
#                "ap-northeast-3",
#                "cn-northwest-1",
#                "eu-central-1",
#            ),
#        }
#        aws_service_region_mapping = get_aws_service_region_mapping(
#            services=services, region_services_json=region_services_json
#        )
#        self.assertDictEqual(
#            expected_aws_service_region_mapping, dict(aws_service_region_mapping),
#        )
#
#    def test_with_unsupported_version(self):
#        services = ("ec2", "elb", "lambda")
#        sample_data_filepath = "tests/data/services_regions/sample_unsupported_version.json"
#        with open(sample_data_filepath, "r") as fp:
#            region_services_json = json.load(fp)
#        with self.assertRaises(UnsupportedServiceRegionMappingVersion):
#            get_aws_service_region_mapping(
#                services=services, region_services_json=region_services_json
#            )
#
#
# class TestGetServiceRegionMapping(unittest.TestCase):
#    def test_with_valid_json_data(self):
#        services = ("ec2", "elb", "lambda")
#        sample_data_filepath = "tests/data/services_regions/sample.json"
#        with open(sample_data_filepath, "r") as fp:
#            region_services_json = json.load(fp)
#        with unittest.mock.patch(
#            "altimeter.aws.scan.services_regions.get_aws_service_region_mapping_json"
#        ) as mock_get_aws_service_region_mapping_json:
#            mock_get_aws_service_region_mapping_json.return_value = region_services_json
#            service_region_mapping = get_service_region_mapping(
#                services=services, services_regions_json_url="mocked"
#            )
#            expected_service_region_mapping = {
#                "ec2": (
#                    "af-south-1",
#                    "ap-northeast-3",
#                    "ap-south-1",
#                    "eu-north-1",
#                    "eu-west-2",
#                    "sa-east-1",
#                    "us-gov-west-1",
#                    "ap-northeast-2",
#                    "ap-southeast-1",
#                    "cn-north-1",
#                    "eu-south-1",
#                    "us-east-1",
#                    "us-east-2",
#                    "us-gov-east-1",
#                    "us-west-2",
#                    "ca-central-1",
#                    "cn-northwest-1",
#                    "eu-central-1",
#                    "us-west-1",
#                ),
#                "elb": (
#                    "ap-northeast-1",
#                    "ap-northeast-3",
#                    "ap-south-1",
#                    "cn-north-1",
#                    "eu-south-1",
#                    "eu-west-2",
#                    "me-south-1",
#                    "us-east-2",
#                    "us-west-2",
#                    "af-south-1",
#                    "ap-northeast-2",
#                    "cn-northwest-1",
#                    "eu-central-1",
#                    "eu-west-1",
#                    "eu-west-3",
#                    "us-east-1",
#                    "us-gov-west-1",
#                    "ap-southeast-1",
#                    "ap-southeast-2",
#                    "ca-central-1",
#                    "us-west-1",
#                ),
#                "lambda": (
#                    "ap-east-1",
#                    "ap-northeast-2",
#                    "ap-south-1",
#                    "ap-southeast-1",
#                    "ap-southeast-2",
#                    "me-south-1",
#                    "us-east-1",
#                    "us-east-2",
#                    "us-gov-west-1",
#                    "ap-northeast-1",
#                    "ca-central-1",
#                    "eu-north-1",
#                    "eu-south-1",
#                    "eu-west-1",
#                    "eu-west-3",
#                    "sa-east-1",
#                    "us-west-2",
#                    "af-south-1",
#                    "ap-northeast-3",
#                    "cn-northwest-1",
#                    "eu-central-1",
#                ),
#            }
#            self.assertDictEqual(
#                expected_service_region_mapping, dict(service_region_mapping),
#            )
#
#    def test_with_unsupported_version_json_data(self):
#        services = ("ec2", "elb", "lambda")
#        sample_data_filepath = "tests/data/services_regions/sample_unsupported_version.json"
#        with open(sample_data_filepath, "r") as fp:
#            region_services_json = json.load(fp)
#        with unittest.mock.patch(
#            "altimeter.aws.scan.services_regions.get_aws_service_region_mapping_json"
#        ) as mock_get_aws_service_region_mapping_json:
#            mock_get_aws_service_region_mapping_json.return_value = region_services_json
#            with unittest.mock.patch(
#                "boto3.Session.get_available_regions"
#            ) as mock_boto_session_get_available_regions:
#                mock_boto_session_get_available_regions.return_value = [
#                    "us-east-1",
#                    "us-east-2",
#                    "us-west-1",
#                    "us-west-2",
#                ]
#                service_region_mapping = get_service_region_mapping(
#                    services=services, services_regions_json_url="mocked"
#                )
#                expected_service_region_mapping = {
#                    "ec2": ("us-east-1", "us-east-2", "us-west-1", "us-west-2",),
#                    "elb": ("us-east-1", "us-east-2", "us-west-1", "us-west-2",),
#                    "lambda": ("us-east-1", "us-east-2", "us-west-1", "us-west-2",),
#                }
#                self.assertDictEqual(
#                    expected_service_region_mapping, dict(service_region_mapping),
#                )
#
