"""Resource for EC2Images (AMIs)"""
import time
from typing import Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.account import AccountResourceSpec
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class EC2ImageResourceSpec(EC2ResourceSpec):
    """Resource for EC2Images (AMIs)"""

    type_name = "image"
    schema = Schema(
        ScalarField("Name"),
        ScalarField("Description", optional=True),
        ScalarField("Public"),
        ListField(
            "LaunchPermissions",
            EmbeddedDictField(
                ResourceLinkField("UserId", AccountResourceSpec, optional=True, alti_key="account")
            ),
            alti_key="launch_permission",
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["EC2ImageResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'image_1_arn': {image_1_dict},
             'image_2_arn': {image_2_dict},
             ...}

        Where the dicts represent results from describe_images."""
        images = {}
        resp = client.describe_images(Owners=["self"])
        for image in resp["Images"]:
            image_id = image["ImageId"]
            time.sleep(0.25)  # seems necessary to avoid frequent RequestLimitExceeded
            try:
                perms_resp = client.describe_image_attribute(
                    Attribute="launchPermission", ImageId=image_id
                )
                launch_permissions = perms_resp["LaunchPermissions"]
                image["LaunchPermissions"] = launch_permissions
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=image_id
                )
                images[resource_arn] = image
            except ClientError as c_e:
                response_error = getattr(c_e, "response", {}).get("Error", {})
                error_code = response_error.get("Code", "")
                if error_code == "InvalidAMIID.Unavailable":
                    continue
                raise c_e
        return ListFromAWSResult(resources=images)
