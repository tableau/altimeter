"""Scalar Fields represent field that contain strings, bools or numbers."""
import datetime
from typing import Union, Dict, Any, List

from altimeter.core.graph.field.exceptions import (
    ScalarFieldSourceKeyNotFoundException,
    ScalarFieldValueNotAScalarException,
)
from altimeter.core.graph.field.base import Field, SubField
from altimeter.core.graph.field.util import camel_case_to_snake_case
from altimeter.core.graph.link.links import SimpleLink
from altimeter.core.graph.link.base import Link

SCALAR_TYPES = (str, bool, int, float, datetime.datetime)


class ScalarField(Field):
    """A ScalarField is a field where the input is a JSON object and the corresponding value is a
    string, number or bool.

    Examples:
        A ScalarField with a string value::
            >>> input = {"FieldName": "Value"}
            >>> field = ScalarField("FieldName")
            >>> links = field.parse(data=input, context={})
            >>> print([link.to_dict() for link in links])
            [{'pred': 'field_name', 'obj': 'Value', 'type': 'simple'}]

        A ScalarField with a string value and an alti_key specified::
            >>> input = {"FieldName": "Value"}
            >>> field = ScalarField("FieldName", alti_key="custom_name")
            >>> links = field.parse(data=input, context={})
            >>> print([link.to_dict() for link in links])
            [{'pred': 'custom_name', 'obj': 'Value', 'type': 'simple'}]

        An optional ScalarField with no default value::
            >>> input = {"SomeOtherFieldName": "Value"}
            >>> field = ScalarField("FieldName", optional=True)
            >>> links = field.parse(data=input, context={})
            >>> print([link.to_dict() for link in links])
            []

        An optional ScalarField with a default value::
            >>> input = {"SomeOtherFieldName": "Value"}
            >>> field = ScalarField("FieldName", optional=True, default_value="DefaultValue")
            >>> links = field.parse(data=input, context={})
            >>> print([link.to_dict() for link in links])
            [{'pred': 'field_name', 'obj': 'DefaultValue', 'type': 'simple'}]

    Args:
        source_key: Name of the key in the input JSON
        alti_key: Optional key name to be used in the graph. By default this is set to the source
                  key converted to snake case.
        optional: Whether this key is optional. Defaults to False.
        default_value: A default value to use if the source_key is not present.
    """

    def __init__(
        self,
        source_key: str,
        alti_key: str = None,
        optional: bool = False,
        default_value: Union[str, bool, int, float] = None,
    ):
        self.source_key = source_key
        self.alti_key = alti_key if alti_key else camel_case_to_snake_case(self.source_key)
        self.optional = optional
        self.default_value = default_value

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> List[Link]:
        """Parse this field and return a list of Links.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            List of SimpleLink objects. At most one link will be returned.
        """
        value = data.get(self.source_key)
        if value is None:
            if self.default_value is not None:
                return [SimpleLink(pred=self.alti_key, obj=self.default_value)]
            if self.optional:
                return []
            raise ScalarFieldSourceKeyNotFoundException(
                f"Expected key '{self.source_key}' in data, present keys: {', '.join(data.keys())}"
            )
        if isinstance(value, SCALAR_TYPES):
            return [SimpleLink(pred=self.alti_key, obj=value)]
        raise ScalarFieldValueNotAScalarException(
            (
                f"Expected data for key '{self.source_key}' to be one "
                f"of {SCALAR_TYPES}, is {type(value)}: {value}"
            )
        )


class EmbeddedScalarField(SubField):
    """An EmbeddedScalarField is a field where the input is a string, number or bool.
    An EmbeddedScalarField assumes the key of the enclosing field.

    Examples:
        An EmbeddedScalarField inside a ListField::
            >>> from altimeter.core.graph.field.list_field import ListField
            >>> input = {"Animal": ["Value1", "Value2"]}
            >>> field = ListField("Animal", EmbeddedScalarField())
            >>> links = field.parse(data=input, context={})
            >>> for link in links:
            ...     print(link.to_dict())
            ...
            {'pred': 'animal', 'obj': 'Value1', 'type': 'simple'}
            {'pred': 'animal', 'obj': 'Value2', 'type': 'simple'}
    """

    def parse(self, data: Union[str, bool, int, float], context: Dict[str, Any]) -> List[Link]:
        """Parse this field and return a list of Links. For a ScalarField at most one link
        will be returned.

        Args:
            data: scalar value
            context: context dict containing data from higher level parsing code.

        Returns:
            List of Link objects. At most one link will be returned.
        """
        parent_alti_key = self.get_parent_alti_key(data, context)
        links: List[Link] = []
        if isinstance(data, SCALAR_TYPES):
            link = SimpleLink(pred=parent_alti_key, obj=data)
            links.append(link)
            return links
        raise ScalarFieldValueNotAScalarException(
            (f"Expected data to be one of {SCALAR_TYPES}, is " f"{type(data)}: {data}")
        )
