"""Resource Link Fields represent field containing ids of other top level resources in the graph.
For example, an EC2 instance has a ResourceLinkField with source_key 'VpcId' pointing to a VPC."""
from typing import Dict, Any, Type, Union

from altimeter.core.graph.field.exceptions import (
    ResourceLinkFieldSourceKeyNotFoundException,
    ResourceLinkFieldValueNotAStringException,
)
from altimeter.core.graph.field.base import Field, SubField
from altimeter.core.graph.links import LinkCollection, ResourceLink, TransientResourceLink
from altimeter.core.resource.resource_spec import ResourceSpec


class ResourceLinkField(Field):
    """A ResourceLinkField represents a field containing ids of other top level resources in the
    graph. For example, an EC2 instance has a ResourceLinkField with source_key 'VpcId' pointing to
    a VPC.

    Examples:
        A link to a TestResourceSpec resource::
            >>> from altimeter.core.resource.resource_spec import ResourceSpec
            >>> class TestResourceSpec(ResourceSpec): type_name="thing"
            >>> input = {"ThingId": "123"}
            >>> field = ResourceLinkField("ThingId", TestResourceSpec)
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'resource_links': ({'pred': 'thing', 'obj': 'thing:123'},)}

        A link to a TestResourceSpec resource using value_is_id::
            >>> from altimeter.core.resource.resource_spec import ResourceSpec
            >>> class TestResourceSpec(ResourceSpec): type_name="thing"
            >>> input = {"ThingId": "thing:123"}
            >>> field = ResourceLinkField("ThingId", TestResourceSpec, value_is_id=True)
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'resource_links': ({'pred': 'thing', 'obj': 'thing:123'},)}

    Args:
        source_key: Name of the key in the input JSON
        resource_spec_class: The name of the ResourceSpec class or the ResourceSpec class which this
                             link represents.
        alti_key: Optional key name to be used in the graph. By default this is set to the
                  resource_spec_class' type_name attribute.
        optional: Whether this key is optional. Defaults to False.
        value_is_id: Whether the value for this key contains the entire resource id. For AWS
                     resources set this to True if the value is a complete arn.
    """

    def __init__(
        self,
        source_key: str,
        resource_spec_class: Union[Type[ResourceSpec], str],
        alti_key: str = None,
        optional: bool = False,
        value_is_id: bool = False,
    ):
        self.source_key = source_key
        self._resource_spec_class = resource_spec_class
        self.alti_key = alti_key
        self.optional = optional
        self.value_is_id = value_is_id

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a LinkCollection

        Args:
            data: data to parse
            context: contains data from higher level parsing code.

        Returns:
             LinkCollection
        """
        if isinstance(self._resource_spec_class, str):
            resource_spec_class: Type[ResourceSpec] = ResourceSpec.get_by_class_name(
                self._resource_spec_class
            )
        else:
            resource_spec_class = self._resource_spec_class
        if not self.alti_key:
            self.alti_key = resource_spec_class.type_name
        short_resource_id = data.get(self.source_key)
        if not short_resource_id:
            if self.optional:
                return LinkCollection()
            raise ResourceLinkFieldSourceKeyNotFoundException(
                f"Expected key '{self.source_key}' with non-empty/zero value in {data}"
            )
        if not isinstance(short_resource_id, str):
            raise ResourceLinkFieldValueNotAStringException(
                (
                    f"ResourceLinkField for {self.source_key} expected a string but got a "
                    f"{type(short_resource_id)} : {short_resource_id}"
                )
            )
        if self.value_is_id:
            resource_id = short_resource_id
        else:
            resource_id = resource_spec_class.generate_id(short_resource_id, context)
        return LinkCollection(resource_links=[ResourceLink(pred=self.alti_key, obj=resource_id)],)


class EmbeddedResourceLinkField(SubField):
    """An EmbeddedResourceLinkField is a ResourceLinkField where the input is the resource id
    only, not a key/value where the value is a resource id.

    Examples:
        A link to a TestResourceSpec resource::
            >>> from altimeter.core.graph.field.list_field import ListField
            >>> from altimeter.core.resource.resource_spec import ResourceSpec
            >>> class TestResourceSpec(ResourceSpec): type_name="thing"
            >>> input = {"Thing": ["123", "456"]}
            >>> field = ListField("Thing", EmbeddedResourceLinkField(TestResourceSpec))
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'resource_links': ({'pred': 'thing', 'obj': 'thing:123'}, {'pred': 'thing', 'obj': 'thing:456'})}

    Args:
        resource_spec_class: The name of the ResourceSpec class or the ResourceSpec class which
                             this link represents.
        optional: Whether this key is optional. Defaults to False.
        value_is_id: Whether the value for this key contains the entire resource id. For AWS
                     resources set this to True if the value is a complete arn.
    """

    def __init__(
        self,
        resource_spec_class: Union[Type[ResourceSpec], str],
        alti_key: str = None,
        optional: bool = False,
        value_is_id: bool = False,
    ):
        self._resource_spec_class = resource_spec_class
        self.alti_key = alti_key
        self.optional = optional
        self.value_is_id = value_is_id

    def parse(self, data: str, context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a LinkCollection.

        Args:
            data: data to parse
            context: contains data from higher level parsing code.

        Returns:
            LinkCollection
        """
        if isinstance(self._resource_spec_class, str):
            resource_spec_class: Type[ResourceSpec] = ResourceSpec.get_by_class_name(
                self._resource_spec_class
            )
        else:
            resource_spec_class = self._resource_spec_class
        if not self.alti_key:
            self.alti_key = resource_spec_class.type_name

        short_resource_id = data
        if self.value_is_id:
            resource_id = short_resource_id
        else:
            resource_id = resource_spec_class.generate_id(short_resource_id, context)
        return LinkCollection(resource_links=[ResourceLink(pred=self.alti_key, obj=resource_id)],)


class TransientResourceLinkField(Field):
    """Transient Resource Link Fields represent field containing ids of other top level resources in
    the graph which may not exist. For example, a Lambda can refer to a VPC however VPCs can be
    deleted from lambdas.


    Args:
        source_key (str): Name of the key in the input JSON
        resource_spec_class (str|Type[ResourceSpec]): The name of the ResourceSpec class or the
            ResourceSpec class which this link represents.
        alti_key (str): Optional key name to be used in the graph. By default
            this is set to the resource_spec_class' type_name attribute.
        optional (bool): Whether this key is optional. Defaults to False.
        value_is_id(bool): Whether the value for this key contains the entire resource id.
            For AWS resources set this to True if the value is a complete arn.

    Examples:
        A link to a TestResourceSpec resource::
            >>> from altimeter.core.resource.resource_spec import ResourceSpec
            >>> class TestResourceSpec(ResourceSpec): type_name="thing"
            >>> input = {"ThingId": "123"}
            >>> field = TransientResourceLinkField("ThingId", TestResourceSpec)
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'transient_resource_links': ({'pred': 'thing', 'obj': 'thing:123'},)}
    """

    def __init__(
        self,
        source_key: str,
        resource_spec_class: Union[Type[ResourceSpec], str],
        alti_key: str = None,
        optional: bool = False,
        value_is_id: bool = False,
    ):
        self.source_key = source_key
        self._resource_spec_class = resource_spec_class
        self.alti_key = alti_key
        self.optional = optional
        self.value_is_id = value_is_id

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a LinkCollection.

        Args:
            data: data to parse
            context: contains data from higher level parsing code.

        Returns:
            LinkCollection
        """
        if isinstance(self._resource_spec_class, str):
            resource_spec_class: Type[ResourceSpec] = ResourceSpec.get_by_class_name(
                self._resource_spec_class
            )
        else:
            resource_spec_class = self._resource_spec_class
        if not self.alti_key:
            self.alti_key = resource_spec_class.type_name
        short_resource_id = data.get(self.source_key)
        if not short_resource_id:
            if self.optional:
                return LinkCollection()
            raise ResourceLinkFieldSourceKeyNotFoundException(
                f"Expected key '{self.source_key}' with non-empty/zero value in {data}"
            )
        if self.value_is_id:
            resource_id = short_resource_id
        else:
            resource_id = resource_spec_class.generate_id(short_resource_id, context)
        return LinkCollection(
            transient_resource_links=[TransientResourceLink(pred=self.alti_key, obj=resource_id)],
        )
