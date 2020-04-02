"""Resource for GuardDuty Detectors"""
from typing import Any, Dict, List, Type

from botocore.client import BaseClient

from altimeter.aws.resource.guardduty import GuardDutyResourceSpec
from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.core.graph.field.resource_link_field import TransientResourceLinkField
from altimeter.core.graph.field.dict_field import AnonymousDictField, EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class DetectorResourceSpec(GuardDutyResourceSpec):
    """Resource for GuardDuty Detectors"""

    type_name = "detector"
    schema = Schema(
        ScalarField("CreatedAt"),
        ScalarField("FindingPublishingFrequency"),
        ScalarField("ServiceRole"),
        ScalarField("Status"),
        ScalarField("UpdatedAt"),
        ListField(
            "Members",
            EmbeddedDictField(
                TransientResourceLinkField("DetectorArn", "DetectorResourceSpec", value_is_id=True),
                ScalarField("Email"),
                ScalarField("RelationshipStatus"),
                ScalarField("InvitedAt"),
                ScalarField("UpdatedAt"),
            ),
            alti_key="member",
        ),
        AnonymousDictField(
            "Master",
            ScalarField("AccountId", alti_key="master_account_id"),
            ScalarField("RelationshipStatus", alti_key="master_relationship_status"),
            ScalarField("InvitedAt", alti_key="master_invited_at"),
            optional=True,
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["DetectorResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'detector_1_arn': {detector_1_dict},
             'detector_2_arn': {detector_2_dict},
             ...}

        Where the dicts represent results from list_detectors and list_members, get_detector for
        each listed detector."""
        list_detectors_paginator = client.get_paginator("list_detectors")
        detectors = {}
        for list_detectors_resp in list_detectors_paginator.paginate():
            detector_ids = list_detectors_resp["DetectorIds"]
            for detector_id in detector_ids:
                detector_resp = client.get_detector(DetectorId=detector_id)
                detector = {
                    key: detector_resp[key]
                    for key in (
                        "CreatedAt",
                        "FindingPublishingFrequency",
                        "ServiceRole",
                        "Status",
                        "UpdatedAt",
                    )
                }
                list_members_paginator = client.get_paginator("list_members")
                member_resps: List[Dict[str, Any]] = []
                for list_members_resp in list_members_paginator.paginate(DetectorId=detector_id):
                    member_resps += list_members_resp.get("Members", [])
                members = []
                if member_resps:
                    for member_resp in member_resps:
                        member_account_id = member_resp["AccountId"]
                        member_detector_id = member_resp["DetectorId"]
                        member_email = member_resp["Email"]
                        member_relationship_status = member_resp["RelationshipStatus"]
                        member_invited_at = member_resp["InvitedAt"]
                        member_updated_at = member_resp["UpdatedAt"]
                        member_detector_arn = cls.generate_arn(
                            account_id=member_account_id,
                            region=region,
                            resource_id=member_detector_id,
                        )
                        member = {
                            "DetectorArn": member_detector_arn,
                            "Email": member_email,
                            "RelationshipStatus": member_relationship_status,
                            "InvitedAt": member_invited_at,
                            "UpdatedAt": member_updated_at,
                        }
                        members.append(member)
                detector["Members"] = members
                master_account_resp = client.get_master_account(DetectorId=detector_id)
                master_account_dict = master_account_resp.get("Master")
                if master_account_dict:
                    detector["Master"] = {
                        key: master_account_dict[key]
                        for key in ("AccountId", "RelationshipStatus", "InvitedAt",)
                    }
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=detector_id
                )
                detectors[resource_arn] = detector
        return ListFromAWSResult(resources=detectors)
