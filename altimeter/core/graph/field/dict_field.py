"""Dict Fields represent fields which consist of dict-like data."""
from copy import deepcopy
from typing import Dict, Any

from altimeter.core.graph.field.exceptions import (
    DictFieldValueNotADictException,
    DictFieldSourceKeyNotFoundException,
    DictFieldValueIsNullException,
)
from altimeter.core.graph.field.base import Field, SubField
from altimeter.core.graph.field.util import camel_case_to_snake_case
from altimeter.core.graph.links import LinkCollection, MultiLink


class DictField(Field):
    """A DictField is a field where the input is a JSON object containing a key (source_key)
    where the corresponding value is a dictionary.

    Examples:
        A dictionary containing two ScalarFields:
            >>> from altimeter.core.graph.field.scalar_field import ScalarField
            >>> input = {"Person": {"FirstName": "Bob", "LastName": "Smith"}}
            >>> field = DictField("Person", ScalarField("FirstName"), ScalarField("LastName"))
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'multi_links': ({'pred': 'person', 'obj': {'simple_links': ({'pred': 'first_name', 'obj': 'Bob'}, {'pred': 'last_name', 'obj': 'Smith'})}},)}

    Args:
        source_key: Name of the key in the input JSON
        fields: fields inside this DictField
        alti_key: Optional key name to be used in the graph. By default
                  this is set to the source key converted to snake case.
        optional: Whether this key is optional. Defaults to False.
    """

    def __init__(
        self, source_key: str, *fields: Field, alti_key: str = None, optional: bool = False
    ) -> None:
        self.source_key = source_key
        self.alti_key = alti_key if alti_key else camel_case_to_snake_case(self.source_key)
        self.optional = optional
        self.fields = fields

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a LinkCollection.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            LinkCollection

        Raises:
            DictFieldSourceKeyNotFoundException if self.source_key is not in data.
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        if self.source_key not in data:
            if self.optional:
                return LinkCollection()
            raise DictFieldSourceKeyNotFoundException(
                f"Expected key '{self.source_key}' in data, present keys: {', '.join(data.keys())}"
            )
        field_data = data.get(self.source_key, {})
        if not isinstance(field_data, dict):
            raise DictFieldValueNotADictException(
                (
                    f"Key '{self.source_key}' value was expected to "
                    f"contain a dict, actual: {field_data} "
                    f"({type(field_data)})"
                )
            )
        updated_context = deepcopy(context)
        updated_context.update({"parent_alti_key": self.alti_key})
        multi_link_object_link_collection = LinkCollection()
        for field in self.fields:
            multi_link_object_link_collection += field.parse(field_data, updated_context)
        return LinkCollection(
            multi_links=[MultiLink(pred=self.alti_key, obj=multi_link_object_link_collection)]
        )


class AnonymousDictField(Field):
    """An AnonymousDictField is a DictField where the source_key of the field is discarded
    and not used as a name in the resulting graph. See Examples below for more clarity.

    Args:
        source_key: Name of the key in the input JSON
        fields: fields inside this DictField
        optional: Whether this key is optional. Defaults to False.
        nullable: Whether this field's value can be null.

    Examples:
        A dict containing 3 ScalarFields
            >>> from altimeter.core.graph.field.scalar_field import ScalarField
            >>> input = {"Person": {"FirstName": "Bob", "LastName": "Smith"}}
            >>> field = AnonymousDictField("Person", ScalarField("FirstName"), ScalarField("LastName"))
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'simple_links': ({'pred': 'first_name', 'obj': 'Bob'}, {'pred': 'last_name', 'obj': 'Smith'})}
    """

    def __init__(
        self, source_key: str, *fields: Field, optional: bool = False, nullable: bool = False
    ):
        self.source_key = source_key
        self.fields = fields
        self.optional = optional
        self.nullable = nullable

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a LinkCollection.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            LinkCollection

        Raises:
            DictFieldSourceKeyNotFoundException if self.source_key is not in data.
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        if self.source_key in data:
            field_data = data.get(self.source_key, None)
            if field_data is None:
                if self.nullable:
                    return LinkCollection()
                raise DictFieldValueIsNullException(
                    f"Key '{self.source_key} was expected to contain a dict, was null."
                )
            if not isinstance(field_data, dict):
                raise DictFieldValueNotADictException(
                    (
                        f"Key '{self.source_key}' value expected to "
                        f"contain a dict, actual: {field_data} "
                        f"({type(field_data)})"
                    )
                )
            link_collection = LinkCollection()
            for field in self.fields:
                link_collection += field.parse(field_data, context)
            return link_collection
        if self.optional:
            return LinkCollection()
        raise DictFieldSourceKeyNotFoundException(
            f"Expected key '{self.source_key}' in data, present keys: {', '.join(data.keys())}"
        )


class EmbeddedDictField(SubField):
    """An EmbeddedDictField is a field where the input is a JSON object.  Generally this field
    is used inside a ListField.

    Args:
        fields: fields inside this DictField

    Examples:
        A ListField containing an EmbeddedDictField with two ScalarFields:
            >>> from altimeter.core.graph.field.list_field import ListField
            >>> from altimeter.core.graph.field.scalar_field import ScalarField
            >>> input = {"People": [{"FirstName": "Bob", "LastName": "Smith"},\
                         {"FirstName": "Alice", "LastName": "Smith"}]}
            >>> field = ListField("People", EmbeddedDictField(ScalarField("FirstName"),\
                                  ScalarField("LastName")))
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'multi_links': ({'pred': 'people', 'obj': {'simple_links': ({'pred': 'first_name', 'obj': 'Bob'}, {'pred': 'last_name', 'obj': 'Smith'})}}, {'pred': 'people', 'obj': {'simple_links': ({'pred': 'first_name', 'obj': 'Alice'}, {'pred': 'last_name', 'obj': 'Smith'})}})}
    """

    def __init__(self, *fields: Field) -> None:
        self.fields = fields

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a LinkCollection.

        Args:
            data: dictionary of data to parse
            context: context dict containing data from higher level parsing code.

        Returns:
            LinkCollection

        Raises:
            DictFieldSourceKeyNotFoundException if self.source_key is not in data.
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        parent_alti_key = self.get_parent_alti_key(data, context)
        if not isinstance(data, dict):
            raise Exception(f"{type(data)} {data} was expected to be a dict.")
        updated_context = deepcopy(context)
        updated_context.update({"parent_alti_key": parent_alti_key})
        multi_link_object_link_collection = LinkCollection()
        for field in self.fields:
            multi_link_object_link_collection += field.parse(data, context)
        return LinkCollection(
            multi_links=[MultiLink(pred=parent_alti_key, obj=multi_link_object_link_collection)]
        )


class AnonymousEmbeddedDictField(Field):
    """An AnonymousEmbeddedDictField is a EmbeddedDictField where the source_key of the parent
    field is discarded and not used as a name in the resulting graph. See Examples below for more
    clarity.

    Args:
        fields: fields inside this DictField

    Examples:
        A ListField containing an AnonymousEmbeddedDictField with two ScalarFields:
            >>> from altimeter.core.graph.field.list_field import ListField
            >>> from altimeter.core.graph.field.scalar_field import ScalarField
            >>> input = {"People": [{"FirstName": "Bob", "LastName": "Smith"},\
                         {"FirstName": "Alice", "LastName": "Smith"}]}
            >>> field = ListField("People", AnonymousEmbeddedDictField(ScalarField("FirstName"),\
                                  ScalarField("LastName")))
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'simple_links': ({'pred': 'first_name', 'obj': 'Bob'}, {'pred': 'last_name', 'obj': 'Smith'}, {'pred': 'first_name', 'obj': 'Alice'}, {'pred': 'last_name', 'obj': 'Smith'})}
    """

    def __init__(self, *fields: Field) -> None:
        self.fields = fields

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a list of Links.

        Args:
            data: dictionary of data to parse
            context: context dict containing data from higher level parsing code.

        Returns:
            LinkCollection

        Raises:
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        if not isinstance(data, dict):
            raise DictFieldValueNotADictException(f"{type(data)} {data} was expected to be a dict.")
        link_collection = LinkCollection()
        for field in self.fields:
            link_collection += field.parse(data, context)
        return link_collection
