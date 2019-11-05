class EC2InstanceResourceSpec(EC2ResourceSpec):
    type_name = "instance"
    schema = Schema(
        TransientResourceLinkField("ImageId", EC2ImageResourceSpec),
        ScalarField("InstanceType"),
        AnonymousDictField("State", ScalarField("Name", "state")),
        ScalarField("PublicIpAddress", optional=True),
        ResourceLinkField("VpcId", VPCResourceSpec, optional=True),
        ResourceLinkField("SubnetId", SubnetResourceSpec, optional=True),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type[T], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        paginator = client.get_paginator("describe_instances")
        instances = {}
        for resp in paginator.paginate():
            for reservation in resp.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    resource_arn = cls.generate_arn(account_id, region, instance["InstanceId"])
                    instances[resource_arn] = instance
        return ListFromAWSResult(resources=instances)
