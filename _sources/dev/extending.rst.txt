Extending Altimeter
===================

Altimeter is designed to be extendable.  Adding new fields for resource types which
are already scanned and graphed is usually straightforward.  Adding new resource types
which are not yet scanned and graphed involves a bit more work but also usually
simple.

Background
----------

When the Altimeter scan runs (`bin/aws2json.py`) it uses a set of
:class:`ResourceSpec` classes to gather data via AWS APIs (using boto3) and
persist it into an intermediate JSON format which can be converted
to RDF.

Each of these classes must define attributes and methods which define
how exactly data is collected (what boto3 calls, massaging of response
data), how the collected data relates in a graph (a :class:`Schema`) and
taxonomical information (resource type name - e.g. "instance", resource
service name - e.g. "ec2").

Resource Specs
--------------

Each AWS resource type is represented by a subclass
of :class:`altimeter.aws.resource.resource_spec.AWSResourceSpec`.

Subclasses of :class:`AWSResourceSpec` must implement:

    * An attribute `service_name` which should match the name of the resource's
      associated service as represented in an ARN for this resource type.
      For instance for EC2 instances this is "ec2" to match the service portion
      of ec2 ARNs
      "arn:${Partition}:ec2:${Region}:${Account}:instance/${InstanceId}".
    * An attribute `type_name` which should match the type name as represented
      in an ARN for this resource type.  For instance, in
      :class:`altimeter.aws.resource.ec2.instance.EC2InstanceResourceSpec`
      `type_name` is set to "instance" to match the ARN format
      "arn:${Partition}:ec2:${Region}:${Account}:instance/${InstanceId}".
    * An attribute `schema` of type :class:`altimeter.core.graph.schema.Schema`.
      The `schema` attribute describes how to map the output of the boto3 list/describe
      api call for this resource into `fields` which define how the resource is graphed.
    * A method :func:`list_from_aws` which performs the describe/list boto3 calls
      for this resource, compiles a dictionary with resource ARNs as keys
      and resource description dicts as values and wraps them in an
      :class:`altimeter.aws.resource.resource_spec.ListFromAWSResult` object.

For each AWSResourceSpec class Altimeter gathers data using :func:`list_from_aws`
and uses the provided :class:`Schema` object to convert it to intermediate JSON.

At this point it's instructive to look at an example, the below is an abbreviated
version of
:class:`altimeter.aws.resource.ec2.instance.EC2InstanceResourceSpec`:

.. highlight:: python
   :linenothreshold: 40

.. literalinclude:: example_ec2_instance_resource_spec_1.py

As mentioned above, the `type_name` is set to "instance" to match the ARN format of EC2
instances:

.. literalinclude:: example_ec2_instance_resource_spec_1.py
   :lines: 2

Note that the `service_name` attribute mentioned above is implemented in the parent
class :class:`EC2ResourceSpec` which is a direct subclass of :class:`AWSResourceSpec`.

Next lets look at the :func:`list_from_aws` function.  In general this function should
generate a dictionary representing all resources of this type, where keys are
ARNs and values are dictionaries containing information about the resource:

::

   {'resource_1_arn': resource_1_dict,
    'resource_2_arn': resource_2_dict,
    ...}

Here is the implementation for our sample :class:`EC2ResourceSpec` class:

.. literalinclude:: example_ec2_instance_resource_spec_1.py
   :lines: 12-23

This is a fairly typical implementation - it uses the appropriate
list/describe boto3 function for this resource type and massages
the data into a dictionary of ARNs to resource dictionaries. It
then returns a :class:`ListFromAWSResult` object which wraps this dictionary.

The dictionary in this case will look something like the below (abbreviated):

::

   { 'ec2-instance-1-arn':
     { 'AmiLaunchIndex': 123,
        'ImageId': 'string',
        'InstanceId': 'string',
        'InstanceType': 't1.micro|t2.nano|t2.micro|t2.small|t2.medium|...',
        'LaunchTime': datetime(2015, 1, 1),
        'PublicIpAddress': 'string',
        'State': {
            'Code': 123,
            'Name': 'pending|running|shutting-down|terminated|stopping|stopped'
        },
        'VpcId': 'string',
        'Tags': [
            {
                'Key': 'string',
                'Value': 'string'
            },
        ],
      },
     'ec2-instance-2-arn':
       { ...
       },
      ...
   }

Finally lets examine the schema:

.. literalinclude:: example_ec2_instance_resource_spec_1.py
   :lines: 3-10

Altimeter uses this schema definition to translate the output of :func:`list_from_aws` into
intermediate JSON.  The intermediate JSON contains graph relational information
about the object and its attributes.

A Schema consists of a list of :class:`Field` objects.  In this case we are using a few
different kinds of fields.  For all but one (:class:`TagsField`) the first argument
is a key as returned by :func:`list_from_aws`. This argument is known as the
`source_key`.

For example line 5 contains a :class:`ScalarField` which uses the `source_key`
"InstanceType".  :class:`ScalarField` is the simplest kind of field, it is used
when a value is a simple string with no relational data:

.. literalinclude:: example_ec2_instance_resource_spec_1.py
   :lines: 5

Line 8 contains a slightly less simple field type - :class:`ResourceLinkField`:

.. literalinclude:: example_ec2_instance_resource_spec_1.py
   :lines: 8

A :class:`ResourceLinkField` is a field where the value contains the ID of another
resource which is also in the graph.  Since we also graph VPCs using the
:class:`VPCResourceSpec` type, we can represent a link between an EC2 instance
and its correseponding VPC using a :class:`ResourceLinkField` on the parent
(:class:`EC2ResourceSpec`) referencing the child (:class:`VPCResourceSpec`) as the second
argument of the :class:`ResourceLinkField`.  Note that there is an additional keyword
argument `optional` which indicates this field does not always appear.

The various types of fields and how they are used are documented in the API
documentation, see :doc:`altimeter.core.graph.field <../altimeter.core.graph.field>`.
Numerous examples of their use are also provided in the :class:`AWSResourceSpec` subclasses
in `altimeter/aws/resource`.

Modifying an Existing Resource Type
-----------------------------------

Note that the Schema of an :class:`AWSResourceSpec` subclass does not necessarily include
all the data that is in the output of :func:`list_from_aws`.  Some keys are just not all that
interesting to us at this time and are not worth graphing. In general we
add new fields in the graph as we need them.

Adding a new field is as simple as adding a new :class:`Field` object to the
:class:`AWSResourceSpec`'s `schema` attribute. For example, if we would additionally
like to graph the subnet of an ec2 instance we could add a field to pull the `SubnetId` field
which is provided in the `describe_instances` call in :func:`list_from_aws`. Since we
have an :class:`AWSResourceSpec` class for subnets (:class:`SubnetResourceSpec`) we
can use a :class:`ResourceLinkField`. The schema now looks like:

.. literalinclude:: example_ec2_instance_resource_spec_2.py
   :lines: 3-11
   :emphasize-lines: 7

As with the VPC field the new subnet field has the `optional` flag set as not all EC2 instances
have subnets.

Testing Changes
---------------

The quickest way to test a change is to use the provided `bin/scan_resource.py` script.
This script will run a scan of a single :class:`AWSResourceSpec` class against a
single specified AWS region using the currently configured AWS credential profile and
write scan output to STDOUT. For example after the subnet addition above,
running

::

   bin/scan_resource.py EC2InstanceResourceSpec us-east-1

in an account `012345678901` with a single ec2 instance could return something like:

.. literalinclude:: sample_scan_resource_1.json

Adding a New AWSResourceSpec
----------------------------

Adding a new resource type to the graph consists of adding a new class which is
a subclass of :class:`AWSResourceSpec`.

These classes live in :mod:`altimeter.aws.resource`. They are organized by AWS service
(e.g. "ec2", "rds", "s3", etc).

The service packages (for instance :mod:`altimeter.aws.resource.ec2`) define a subclass
of :class:`AWSResourceSpec` which sets the `service_name` attribute.  In the case of
ec2 this is :class:`altimeter.aws.resource.ec2.EC2ResourceSpec`:

.. literalinclude:: ../../../altimeter/aws/resource/ec2/__init__.py

If the resource you wish to add is not within a service already defined under :mod:`altimeter.aws.resource`
you will need to create a class similar to the :class:`EC2ResourceSpec` class above.

When adding a new :class:`AWSResourceSpec` subclass there are a few things you will need to do:

* As alluded to above your class should subclass an existing service-level :class:`AWSResourceSpec` class,
  for example :class:`EC2ResourceSpec`.
* Set the `type_name` attribute using the name as presented in an ARN of the specific resource type (e.g. `instance`).
* Implement :func:`list_from_aws` to gather data and produce a :class:`ListFromAWSResult`.
* Set the `schema` attribute to a :class:`Schema` object with the appropriate fields.
* Add your class to :mod:`altimeter.aws.scan.settings.RESOURCE_SPEC_CLASSES`. This contains a list of enabled
  scan resource types.
* Test using `bin/scan_resource.py`.
* Add tests for the new type in :mod:`tests.unit.altimeter.aws.resource`, preferably using moto. See examples
  in :mod:`tests.unit.altimeter.aws.resource.ec2` for examples of such tests.


