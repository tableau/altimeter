"""List Fields represent fields which consist of list data."""
from copy import deepcopy
from typing import Dict, Any

from altimeter.core.graph.exceptions import (
    ListFieldSourceKeyNotFoundException,
    ListFieldValueNotAListException,
)
from altimeter.core.graph.field.base import Field, SubField
from altimeter.core.graph import SCALAR_TYPES
from altimeter.core.graph.field.util import camel_case_to_snake_case
from altimeter.core.graph.links import LinkCollection


class ListField(Field):
    """A ListField is a field where the input is a JSON object containing a key (source_key)
    where the corresponding value is a list of homogeneous items.

    Examples:
        A list of strings:
            >>> from altimeter.core.graph.field.scalar_field import EmbeddedScalarField
            >>> input = {"Animals": ["cow", "pig", "human"]}
            >>> field = ListField("Animals", EmbeddedScalarField())
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'simple_links': ({'pred': 'animals', 'obj': 'cow'}, {'pred': 'animals', 'obj': 'pig'}, {'pred': 'animals', 'obj': 'human'})}

        A list of dicts:
            >>> from altimeter.core.graph.field.dict_field import EmbeddedDictField
            >>> from altimeter.core.graph.field.scalar_field import ScalarField
            >>> input = {"People": [{"Name": "Bob", "Age": 49}, {"Name": "Sue", "Age": 42}]}
            >>> field = ListField("People", EmbeddedDictField(ScalarField("Name"),\
                                  ScalarField("Age")))
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'multi_links': ({'pred': 'people', 'obj': {'simple_links': ({'pred': 'name', 'obj': 'Bob'}, {'pred': 'age', 'obj': 49})}}, {'pred': 'people', 'obj': {'simple_links': ({'pred': 'name', 'obj': 'Sue'}, {'pred': 'age', 'obj': 42})}})}

    Args:
        source_key: Name of the key in the input JSON
        sub_field: SubField object representing the type that is contained in this list.
        alti_key: Optional key name to be used in the graph. By default
                  this is set to the source key converted to snake case.
        optional: Whether this key is optional. Defaults to False.
        allow_scalar: Whether this field can sometimes contain a scalar rather
                      than a list - if this is set to True the scalar will be treated as a
                      list of one. Defaults to False.
    """

    def __init__(
        self,
        source_key: str,
        sub_field: SubField,
        alti_key: str = None,
        optional: bool = False,
        allow_scalar: bool = False,
    ):
        self.source_key = source_key
        self.sub_field = sub_field
        self.alti_key = alti_key if alti_key else camel_case_to_snake_case(self.source_key)
        self.optional = optional
        self.allow_scalar = allow_scalar

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a LinkCollection

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            LinkCollection

        Raises:
            ListFieldSourceKeyNotFoundException if self.source_key is not in data.
            ListFieldValueNotAListException if the data does not appear to represent a list.
        """
        if self.source_key not in data:
            if self.optional:
                return LinkCollection()
            raise ListFieldSourceKeyNotFoundException(
                f"Expected key '{self.source_key}' in data, present keys: {', '.join(data.keys())}"
            )
        sub_datas = data.get(self.source_key, [])
        if not isinstance(sub_datas, list):
            if self.allow_scalar and isinstance(sub_datas, SCALAR_TYPES):
                sub_datas = [sub_datas]
            else:
                raise ListFieldValueNotAListException(
                    (
                        f"Key '{self.source_key}' value had unexpected type, value: {sub_datas} "
                        f"type: {type(sub_datas)}"
                    )
                )
        link_collection = LinkCollection()
        updated_context = deepcopy(context)
        updated_context.update({"parent_alti_key": self.alti_key})
        for sub_data in sub_datas:
            link_collection += self.sub_field.parse(sub_data, updated_context)
        return link_collection


class AnonymousListField(Field):
    """An AnonymousListField is a ListField where the source_key of the field is discarded
    and not used as a name in the resulting graph. See Examples below for more clarity.

    Args:
        source_key: Name of the key in the input JSON
        field: Field object representing the type that is contained in this list.
        optional: Whether this key is optional. Defaults to False.
        allow_scalar: Whether this field can sometimes contain a scalar rather than a list - if
            this is set to True the scalar will be treated as a list of one. Defaults to False.

    Examples:
        A DictField containing an AnonymousListField:
            >>> from altimeter.core.graph.field.dict_field import DictField
            >>> from altimeter.core.graph.field.scalar_field import EmbeddedScalarField
            >>> input = {"Biota": {"Animals": ["cow", "pig", "human"],\
                                   "Plants": ["tree", "fern"]}}
            >>> field = DictField("Biota", AnonymousListField("Animals", EmbeddedScalarField()))
            >>> link_collection = field.parse(data=input, context={})
            >>> print(link_collection.dict(exclude_unset=True))
            {'multi_links': ({'pred': 'biota', 'obj': {'simple_links': ({'pred': 'biota', 'obj': 'cow'}, {'pred': 'biota', 'obj': 'pig'}, {'pred': 'biota', 'obj': 'human'})}},)}
    """

    def __init__(
        self, source_key: str, field: Field, optional: bool = False, allow_scalar: bool = False
    ):
        self.source_key = source_key
        self.field = field
        self.optional = optional
        self.allow_scalar = allow_scalar

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this field and return a list of Links.

       Args:
           data: dictionary of data to parse
           context: context dict containing data from higher level parsing code.

        Returns:
            LinkCollection

        Raises:
            ListFieldSourceKeyNotFoundException if self.source_key is not in data.
            ListFieldValueNotAListException if the data does not appear to represent a list.
        """
        if self.source_key not in data:
            if self.optional:
                return LinkCollection()
            raise ListFieldSourceKeyNotFoundException(
                f"Expected key '{self.source_key}' in data, present keys: {', '.join(data.keys())}"
            )
        sub_datas = data.get(self.source_key, [])
        if not isinstance(sub_datas, list):
            if self.allow_scalar and isinstance(sub_datas, SCALAR_TYPES):
                sub_datas = [sub_datas]
            else:
                raise ListFieldValueNotAListException(
                    (
                        f"Key '{self.source_key}' value had unexpected type, value: {sub_datas} "
                        f"type: {type(sub_datas)}"
                    )
                )
        link_collection = LinkCollection()
        for sub_data in sub_datas:
            link_collection += self.field.parse(sub_data, context)
        return link_collection
