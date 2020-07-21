"""AWS Resource classes."""
from typing import Tuple, Type

from altimeter.aws.resource.resource_spec import AWSResourceSpec
from altimeter.aws.resource.account import AccountResourceSpec
from altimeter.aws.resource.awslambda.function import LambdaFunctionResourceSpec
from altimeter.aws.resource.dynamodb.dynamodb_table import DynamoDbTableResourceSpec
from altimeter.aws.resource.ec2.flow_log import FlowLogResourceSpec
from altimeter.aws.resource.ec2.image import EC2ImageResourceSpec
from altimeter.aws.resource.ec2.instance import EC2InstanceResourceSpec
from altimeter.aws.resource.ec2.internet_gateway import InternetGatewayResourceSpec
from altimeter.aws.resource.ec2.network_interface import EC2NetworkInterfaceResourceSpec
from altimeter.aws.resource.ec2.region import RegionResourceSpec
from altimeter.aws.resource.ec2.route_table import EC2RouteTableResourceSpec
from altimeter.aws.resource.ec2.transit_gateway_vpc_attachment import (
    TransitGatewayVpcAttachmentResourceSpec,
)
from altimeter.aws.resource.ec2.security_group import SecurityGroupResourceSpec
from altimeter.aws.resource.ec2.snapshot import EBSSnapshotResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.aws.resource.ec2.transit_gateway import TransitGatewayResourceSpec
from altimeter.aws.resource.ec2.volume import EBSVolumeResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.ec2.vpc_endpoint import VpcEndpointResourceSpec
from altimeter.aws.resource.ec2.vpc_peering_connection import VPCPeeringConnectionResourceSpec
from altimeter.aws.resource.elbv1.load_balancer import ClassicLoadBalancerResourceSpec
from altimeter.aws.resource.elbv2.load_balancer import LoadBalancerResourceSpec
from altimeter.aws.resource.elbv2.target_group import TargetGroupResourceSpec
from altimeter.aws.resource.eks.cluster import EKSClusterResourceSpec
from altimeter.aws.resource.events.cloudwatchevents_rule import EventsRuleResourceSpec
from altimeter.aws.resource.events.event_bus import EventBusResourceSpec
from altimeter.aws.resource.guardduty.detector import DetectorResourceSpec
from altimeter.aws.resource.iam.account_password_policy import IAMAccountPasswordPolicyResourceSpec
from altimeter.aws.resource.iam.iam_saml_provider import IAMSAMLProviderResourceSpec
from altimeter.aws.resource.iam.instance_profile import InstanceProfileResourceSpec
from altimeter.aws.resource.iam.policy import IAMPolicyResourceSpec, IAMAWSManagedPolicyResourceSpec
from altimeter.aws.resource.iam.role import IAMRoleResourceSpec
from altimeter.aws.resource.iam.user import IAMUserResourceSpec
from altimeter.aws.resource.kms.key import KMSKeyResourceSpec
from altimeter.aws.resource.organizations.org import OrgResourceSpec
from altimeter.aws.resource.organizations.ou import OUResourceSpec
from altimeter.aws.resource.organizations.account import OrgsAccountResourceSpec
from altimeter.aws.resource.rds.instance import RDSInstanceResourceSpec
from altimeter.aws.resource.rds.snapshot import RDSSnapshotResourceSpec
from altimeter.aws.resource.s3.bucket import S3BucketResourceSpec
from altimeter.aws.resource.support.severity_level import SeverityLevelResourceSpec

# To enable a resource to be scanned, add it here.
RESOURCE_SPEC_CLASSES: Tuple[Type[AWSResourceSpec], ...] = (
    ClassicLoadBalancerResourceSpec,
    DetectorResourceSpec,
    DynamoDbTableResourceSpec,
    EBSSnapshotResourceSpec,
    EBSVolumeResourceSpec,
    EC2ImageResourceSpec,
    EC2InstanceResourceSpec,
    EC2NetworkInterfaceResourceSpec,
    EC2RouteTableResourceSpec,
    EKSClusterResourceSpec,
    EventBusResourceSpec,
    EventsRuleResourceSpec,
    FlowLogResourceSpec,
    IAMAccountPasswordPolicyResourceSpec,
    IAMAWSManagedPolicyResourceSpec,
    IAMPolicyResourceSpec,
    IAMRoleResourceSpec,
    IAMSAMLProviderResourceSpec,
    IAMUserResourceSpec,
    InstanceProfileResourceSpec,
    InternetGatewayResourceSpec,
    KMSKeyResourceSpec,
    LambdaFunctionResourceSpec,
    LoadBalancerResourceSpec,
    RDSInstanceResourceSpec,
    RDSSnapshotResourceSpec,
    S3BucketResourceSpec,
    SecurityGroupResourceSpec,
    SeverityLevelResourceSpec,
    SubnetResourceSpec,
    TargetGroupResourceSpec,
    TransitGatewayResourceSpec,
    TransitGatewayVpcAttachmentResourceSpec,
    VPCPeeringConnectionResourceSpec,
    VPCResourceSpec,
    VpcEndpointResourceSpec,
)

INFRA_RESOURCE_SPEC_CLASSES: Tuple[Type[AWSResourceSpec], ...] = (
    AccountResourceSpec,
    RegionResourceSpec,
)

ORG_RESOURCE_SPEC_CLASSES: Tuple[Type[AWSResourceSpec], ...] = (
    OrgResourceSpec,
    OrgsAccountResourceSpec,
    OUResourceSpec,
)
