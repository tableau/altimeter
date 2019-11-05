"""Dict Fields represent fields which consist of dict-like data."""
from copy import deepcopy
from typing import Dict, Any, List

from altimeter.core.graph.field.exceptions import (
    DictFieldValueNotADictException,
    DictFieldSourceKeyNotFoundException,
    DictFieldValueIsNullException,
)
from altimeter.core.graph.field.base import Field, SubField
from altimeter.core.graph.field.util import camel_case_to_snake_case
from altimeter.core.graph.link.links import MultiLink
from altimeter.core.graph.link.base import Link


class DictField(Field):
    """A DictField is a field where the input is a JSON object containing a key (source_key)
    where the corresponding value is a dictionary.

    Examples:
        A dictionary containing two ScalarFields:
            >>> from altimeter.core.graph.field.scalar_field import ScalarField
            >>> input = {"Person": {"FirstName": "Bob", "LastName": "Smith"}}
            >>> field = DictField("Person", ScalarField("FirstName"), ScalarField("LastName"))
            >>> links = field.parse(data=input, context={})
            >>> print([link.to_dict() for link in links])
            [{'pred': 'person', 'obj': [{'pred': 'first_name', 'obj': 'Bob', 'type': 'simple'}, {'pred': 'last_name', 'obj': 'Smith', 'type': 'simple'}], 'type': 'multi'}]

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

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> List[Link]:
        """Parse this field and return a list of Links.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            List of MultiLink objects.

        Raises:
            DictFieldSourceKeyNotFoundException if self.source_key is not in data.
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        if self.source_key not in data:
            if self.optional:
                return []
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
        links: List[Link] = []
        updated_context = deepcopy(context)
        updated_context.update({"parent_alti_key": self.alti_key})
        for field in self.fields:
            sub_links = field.parse(field_data, updated_context)
            links += sub_links
        return [MultiLink(pred=self.alti_key, obj=links)]


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
            >>> links = field.parse(data=input, context={})
            >>> for link in links: print(link.to_dict())
            {'pred': 'first_name', 'obj': 'Bob', 'type': 'simple'}
            {'pred': 'last_name', 'obj': 'Smith', 'type': 'simple'}
    """

    def __init__(
        self, source_key: str, *fields: Field, optional: bool = False, nullable: bool = False
    ):
        self.source_key = source_key
        self.fields = fields
        self.optional = optional
        self.nullable = nullable

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> List[Link]:
        """Parse this field and return a list of Links.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            List of Link objects.

        Raises:
            DictFieldSourceKeyNotFoundException if self.source_key is not in data.
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        if self.source_key in data:
            field_data = data.get(self.source_key, None)
            if field_data is None:
                if self.nullable:
                    return []
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
            links: List[Link] = []
            for field in self.fields:
                sub_links = field.parse(field_data, context)
                links += sub_links
            return links
        if self.optional:
            return []
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
            >>> links = field.parse(data=input, context={})
            >>> for link in links: print(link.to_dict())
            {'pred': 'people', 'obj': [{'pred': 'first_name', 'obj': 'Bob', 'type': 'simple'}, {'pred': 'last_name', 'obj': 'Smith', 'type': 'simple'}], 'type': 'multi'}
            {'pred': 'people', 'obj': [{'pred': 'first_name', 'obj': 'Alice', 'type': 'simple'}, {'pred': 'last_name', 'obj': 'Smith', 'type': 'simple'}], 'type': 'multi'}
    """

    def __init__(self, *fields: Field) -> None:
        self.fields = fields

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> List[Link]:
        """Parse this field and return a list of Links.

        Args:
            data: dictionary of data to parse
            context: context dict containing data from higher level parsing code.

        Returns:
            List of MultiLink objects.

        Raises:
            DictFieldSourceKeyNotFoundException if self.source_key is not in data.
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        parent_alti_key = self.get_parent_alti_key(data, context)
        if not isinstance(data, dict):
            raise Exception(f"{type(data)} {data} was expected to be a dict.")
        links: List[Link] = []
        updated_context = deepcopy(context)
        updated_context.update({"parent_alti_key": parent_alti_key})
        for field in self.fields:
            sub_links = field.parse(data, updated_context)
            links += sub_links
        return [MultiLink(pred=parent_alti_key, obj=links)]


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
            >>> links = field.parse(data=input, context={})
            >>> for link in links: print(link.to_dict())
            {'pred': 'first_name', 'obj': 'Bob', 'type': 'simple'}
            {'pred': 'last_name', 'obj': 'Smith', 'type': 'simple'}
            {'pred': 'first_name', 'obj': 'Alice', 'type': 'simple'}
            {'pred': 'last_name', 'obj': 'Smith', 'type': 'simple'}
    """

    def __init__(self, *fields: Field) -> None:
        self.fields = fields

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> List[Link]:
        """Parse this field and return a list of Links.

        Args:
            data: dictionary of data to parse
            context: context dict containing data from higher level parsing code.

        Returns:
            List of Link objects.

        Raises:
            DictFieldValueNotADictException if the data does not appear to represent a dict.
        """
        if not isinstance(data, dict):
            raise DictFieldValueNotADictException(f"{type(data)} {data} was expected to be a dict.")
        links: List[Link] = []
        for field in self.fields:
            sub_links = field.parse(data, context)
            links += sub_links
        return links
