from unittest import TestCase

import boto3
from moto import mock_s3

from altimeter.aws.resource.s3.bucket import (
    get_s3_bucket_encryption,
    get_s3_bucket_region,
    get_s3_bucket_tags,
    S3BucketDoesNotExistException,
)


class TestGetS3BucketRegion(TestCase):
    @mock_s3
    def test_with_existing(self):
        bucket_name = "test-bucket"

        session = boto3.Session()
        client = session.client("s3")

        client.create_bucket(
            Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
        )

        expected_region = "us-west-1"
        region = get_s3_bucket_region(client=client, bucket_name=bucket_name)

        self.assertEqual(region, expected_region)

    @mock_s3
    def test_with_non_existing(self):
        bucket_name = "test-bucket"

        session = boto3.Session()
        client = session.client("s3")

        with self.assertRaises(S3BucketDoesNotExistException):
            get_s3_bucket_region(client=client, bucket_name=bucket_name)


class TestGetS3BucketTags(TestCase):
    @mock_s3
    def test_with_existing_with_tags(self):
        bucket_name = "test-bucket"

        session = boto3.Session()
        client = session.client("s3")

        client.create_bucket(
            Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
        )
        client.put_bucket_tagging(
            Bucket="test-bucket",
            Tagging={
                "TagSet": [
                    {"Key": "key1", "Value": "val1"},
                    {"Key": "key2", "Value": "val2"},
                    {"Key": "key3", "Value": "val3"},
                ]
            },
        )

        expected_tags = [
            {"Key": "key1", "Value": "val1"},
            {"Key": "key2", "Value": "val2"},
            {"Key": "key3", "Value": "val3"},
        ]
        tags = get_s3_bucket_tags(client=client, bucket_name=bucket_name)
        self.assertEqual(tags, expected_tags)

    @mock_s3
    def test_with_existing_no_tags(self):
        bucket_name = "test-bucket"

        session = boto3.Session()
        client = session.client("s3")

        client.create_bucket(
            Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
        )

        expected_tags = []
        tags = get_s3_bucket_tags(client=client, bucket_name=bucket_name)

        self.assertEqual(tags, expected_tags)

    @mock_s3
    def test_with_non_existing(self):
        bucket_name = "test-bucket"

        session = boto3.Session()
        client = session.client("s3")

        with self.assertRaises(S3BucketDoesNotExistException):
            get_s3_bucket_tags(client=client, bucket_name=bucket_name)


class TestGetS3BucketEncryption(TestCase):
    @mock_s3
    def test_with_existing_no_encryption(self):
        bucket_name = "test-bucket"

        session = boto3.Session()
        client = session.client("s3")

        client.create_bucket(
            Bucket="test-bucket", CreateBucketConfiguration={"LocationConstraint": "us-west-1"}
        )

        expected_encryption = {"Rules": []}
        encryption = get_s3_bucket_encryption(client=client, bucket_name=bucket_name)

        self.assertEqual(encryption, expected_encryption)

    #   MOTO-NOT-IMPLEMENTED
    #    @mock_s3
    #    def test_with_existing_with_encryption(self):

    @mock_s3
    def test_with_non_existing(self):
        bucket_name = "test-bucket"

        session = boto3.Session()
        client = session.client("s3")

        with self.assertRaises(S3BucketDoesNotExistException):
            get_s3_bucket_encryption(client=client, bucket_name=bucket_name)
