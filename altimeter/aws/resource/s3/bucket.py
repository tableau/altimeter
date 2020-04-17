"""Resource for S3Buckets"""
from typing import Dict, List, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.resource.resource_spec import ScanGranularity, ListFromAWSResult
from altimeter.aws.resource.s3 import S3ResourceSpec
from altimeter.aws.resource.kms.key import KMSKeyResourceSpec
from altimeter.core.exceptions import AltimeterException
from altimeter.core.graph.field.dict_field import AnonymousDictField, EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema
from altimeter.core.log import Logger


class S3BucketAccessDeniedException(AltimeterException):
    """An Access Denied error occured."""


class S3BucketDoesNotExistException(AltimeterException):
    """A bucket does not exist."""


class S3BucketResourceSpec(S3ResourceSpec):
    """Resource for S3 Buckets"""

    type_name = "bucket"
    scan_granularity = ScanGranularity.ACCOUNT
    schema = Schema(
        ScalarField("Name"),
        ScalarField("CreationDate"),
        AnonymousDictField(
            "ServerSideEncryption",
            ListField(
                "Rules",
                EmbeddedDictField(
                    AnonymousDictField(
                        "ApplyServerSideEncryptionByDefault",
                        ScalarField("SSEAlgorithm", "algorithm"),
                    ),
                    AnonymousDictField(
                        "ApplyServerSideEncryptionByDefault",
                        ResourceLinkField("KMSMasterKeyID", KMSKeyResourceSpec, optional=True),
                    ),
                ),
                alti_key="server_side_default_encryption_rule",
            ),
        ),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["S3BucketResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'bucket_1_arn': {bucket_1_dict},
             'bucket_2_arn': {bucket_2_dict},
             ...}

        Where the dicts represent results from list_buckets."""
        logger = Logger()
        buckets = {}
        buckets_resp = client.list_buckets()
        for bucket in buckets_resp.get("Buckets", []):
            bucket_name = bucket["Name"]
            try:
                try:
                    bucket_region = get_s3_bucket_region(client, bucket_name)
                except S3BucketAccessDeniedException as s3ade:
                    logger.warn(
                        event=AWSLogEvents.ScanAWSResourcesNonFatalError,
                        msg=f"Unable to determine region for {bucket_name}: {s3ade}",
                    )
                    continue
                try:
                    bucket["Tags"] = get_s3_bucket_tags(client, bucket_name)
                except S3BucketAccessDeniedException as s3ade:
                    bucket["Tags"] = []
                    logger.warn(
                        event=AWSLogEvents.ScanAWSResourcesNonFatalError,
                        msg=f"Unable to determine tags for {bucket_name}: {s3ade}",
                    )
                try:
                    bucket["ServerSideEncryption"] = get_s3_bucket_encryption(client, bucket_name)
                except S3BucketAccessDeniedException as s3ade:
                    bucket["ServerSideEncryption"] = {"Rules": []}
                    logger.warn(
                        event=AWSLogEvents.ScanAWSResourcesNonFatalError,
                        msg=f"Unable to determine encryption status for {bucket_name}: {s3ade}",
                    )
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=bucket_region, resource_id=bucket_name
                )
                buckets[resource_arn] = bucket
            except S3BucketDoesNotExistException as s3bdnee:
                logger.warn(
                    event=AWSLogEvents.ScanAWSResourcesNonFatalError,
                    msg=f"{bucket_name}: No longer exists: {s3bdnee}",
                )
        return ListFromAWSResult(resources=buckets)


def get_s3_bucket_region(client: BaseClient, bucket_name: str) -> str:
    """Get S3 bucket region"""
    try:
        region = client.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
        if region is None:
            region = "us-east-1"  # aws api wart
        return region
    except ClientError as c_e:
        response_error = getattr(c_e, "response", {}).get("Error", {})
        error_code = response_error.get("Code", "")
        if error_code == "AccessDenied":
            raise S3BucketAccessDeniedException(
                f"Error getting region for {bucket_name}: {response_error}", c_e
            )
        if error_code == "NoSuchBucket":
            raise S3BucketDoesNotExistException(
                f"Error getting region for {bucket_name}: {response_error}", c_e
            )
        raise c_e


def get_s3_bucket_tags(client: BaseClient, bucket_name: str) -> List[Dict[str, str]]:
    """Get S3 bucket tagging, handle the fact that this call returns an error
    if a bucket has no tags."""
    try:
        return client.get_bucket_tagging(Bucket=bucket_name).get("TagSet", [])
    except ClientError as c_e:
        response_error = getattr(c_e, "response", {}).get("Error", {})
        error_code = response_error.get("Code", "")
        if error_code == "NoSuchTagSet":
            return []
        if error_code == "AccessDenied":
            raise S3BucketAccessDeniedException(
                f"Error getting tags for {bucket_name}: {response_error}", c_e
            )
        if error_code == "NoSuchBucket":
            raise S3BucketDoesNotExistException(
                f"Error getting tags for {bucket_name}: {response_error}", c_e
            )
        raise c_e


def get_s3_bucket_encryption(
    client: BaseClient, bucket_name: str
) -> Dict[str, List[Dict[str, str]]]:
    """Returns encryption configuration rules for the bucket."""
    try:
        config = client.get_bucket_encryption(Bucket=bucket_name)
        if (
            "ServerSideEncryptionConfiguration" in config
            and "Rules" in config["ServerSideEncryptionConfiguration"]
        ):
            return config["ServerSideEncryptionConfiguration"]
        return {"Rules": []}
    except ClientError as c_e:
        response_error = getattr(c_e, "response", {}).get("Error", {})
        error_code = response_error.get("Code", "")
        if error_code == "AccessDenied":
            raise S3BucketAccessDeniedException(
                f"Error getting encryption configuration for {bucket_name}: {response_error}", c_e
            )
        if error_code == "NoSuchBucket":
            raise S3BucketDoesNotExistException(
                f"Error getting encryption configuration for {bucket_name}: {response_error}", c_e
            )
        if error_code == "ServerSideEncryptionConfigurationNotFoundError":
            return {"Rules": []}
        raise c_e
