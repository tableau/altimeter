import unittest

from altimeter.aws.resource.ec2.route_table import EC2RouteTableResourceSpec
from altimeter.core.graph.links import LinkCollection, MultiLink, ResourceLink, SimpleLink
from altimeter.core.resource.resource import Resource


class TestRouteTableSchema(unittest.TestCase):
    def test_schema_parse(self):
        resource_arn = "arn:aws:ec2:us-east-2:111122223333:route-table/rtb-099c7b032f2bbddda"
        aws_resource_dict = {
            "Associations": [
                {
                    "Main": False,
                    "RouteTableAssociationId": "rtbassoc-069d59127bf10a728",
                    "RouteTableId": "rtb-099c7b032f2bbddda",
                    "SubnetId": "subnet-00f9fe55b9d7ca4fb",
                },
                {
                    "Main": False,
                    "RouteTableAssociationId": "rtbassoc-07bfd170c4ece33c8",
                    "RouteTableId": "rtb-099c7b032f2bbddda",
                    "SubnetId": "subnet-0b98092b454c882cf",
                },
            ],
            "PropagatingVgws": [],
            "RouteTableId": "rtb-099c7b032f2bbddda",
            "Routes": [
                {
                    "DestinationCidrBlock": "172.31.0.0/16",
                    "GatewayId": "local",
                    "Origin": "CreateRouteTable",
                    "State": "active",
                },
                {
                    "DestinationCidrBlock": "0.0.0.0/0",
                    "GatewayId": "igw-092e5ec1685fd0c0b",
                    "Origin": "CreateRoute",
                    "State": "active",
                },
                {
                    "DestinationPrefixListId": "pl-68a54001",
                    "GatewayId": "vpce-0678bce2b63b8ad0f",
                    "Origin": "CreateRoute",
                    "State": "active",
                },
            ],
            "VpcId": "vpc-03c33051f57d21ff0",
            "OwnerId": "210554966933",
        }

        link_collection = EC2RouteTableResourceSpec.schema.parse(
            data=aws_resource_dict, context={"account_id": "111122223333", "region": "us-west-2"}
        )
        resource = Resource(
            resource_id=resource_arn,
            type=EC2RouteTableResourceSpec.type_name,
            link_collection=link_collection,
        )

        expected_alti_resource = Resource(
            resource_id="arn:aws:ec2:us-east-2:111122223333:route-table/rtb-099c7b032f2bbddda",
            type="route-table",
            link_collection=LinkCollection(
                simple_links=(
                    SimpleLink(pred="route_table_id", obj="rtb-099c7b032f2bbddda"),
                    SimpleLink(pred="owner_id", obj="210554966933"),
                ),
                multi_links=(
                    MultiLink(
                        pred="route",
                        obj=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="destination_cidr_block", obj="172.31.0.0/16"),
                                SimpleLink(pred="gateway_id", obj="local"),
                                SimpleLink(pred="origin", obj="CreateRouteTable"),
                                SimpleLink(pred="state", obj="active"),
                            ),
                        ),
                    ),
                    MultiLink(
                        pred="route",
                        obj=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="destination_cidr_block", obj="0.0.0.0/0"),
                                SimpleLink(pred="gateway_id", obj="igw-092e5ec1685fd0c0b"),
                                SimpleLink(pred="origin", obj="CreateRoute"),
                                SimpleLink(pred="state", obj="active"),
                            ),
                        ),
                    ),
                    MultiLink(
                        pred="route",
                        obj=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="destination_prefix_list_id", obj="pl-68a54001"),
                                SimpleLink(pred="gateway_id", obj="vpce-0678bce2b63b8ad0f"),
                                SimpleLink(pred="origin", obj="CreateRoute"),
                                SimpleLink(pred="state", obj="active"),
                            ),
                        ),
                    ),
                    MultiLink(
                        pred="association",
                        obj=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="main", obj=False),
                                SimpleLink(
                                    pred="route_table_association_id",
                                    obj="rtbassoc-069d59127bf10a728",
                                ),
                                SimpleLink(pred="route_table_id", obj="rtb-099c7b032f2bbddda"),
                                SimpleLink(pred="subnet_id", obj="subnet-00f9fe55b9d7ca4fb"),
                            ),
                        ),
                    ),
                    MultiLink(
                        pred="association",
                        obj=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="main", obj=False),
                                SimpleLink(
                                    pred="route_table_association_id",
                                    obj="rtbassoc-07bfd170c4ece33c8",
                                ),
                                SimpleLink(pred="route_table_id", obj="rtb-099c7b032f2bbddda"),
                                SimpleLink(pred="subnet_id", obj="subnet-0b98092b454c882cf"),
                            ),
                        ),
                    ),
                ),
                resource_links=(
                    ResourceLink(
                        pred="vpc",
                        obj="arn:aws:ec2:us-west-2:111122223333:vpc/vpc-03c33051f57d21ff0",
                    ),
                ),
            ),
        )

        self.assertEqual(resource, expected_alti_resource)
