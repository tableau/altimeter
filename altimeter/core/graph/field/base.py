"""Base classes for Fields.  Fields define how individual elements of input JSON are parsed
into a LinkCollection."""
import abc
from typing import Any, Dict

from altimeter.core.graph.field.exceptions import (
    ParentKeyMissingException,
    InvalidParentKeyException,
)
from altimeter.core.graph.links import LinkCollection


class Field(abc.ABC):
    """Abstract base class for all fields"""

    @abc.abstractmethod
    def parse(self, data: Any, context: Dict[str, Any]) -> LinkCollection:
        """Parse data into a LinkCollection using this field's definition."""


class SubField(Field):
    """SubFields are fields which must have a non-anonymous parent Field."""

    def get_parent_alti_key(self, data: Any, context: Dict[str, Any]) -> str:
        """Get the alti_key of the parent of this SubField.

       Args:
           data: field data
           context: contains auxiliary information which can be passed through the parse process.

        Returns:
            alti_key of parent of this SubField
        """
        parent_alti_key = context.get("parent_alti_key")
        if parent_alti_key is None:
            raise ParentKeyMissingException(
                (
                    f"Missing parent_alti_key in context for "
                    f"{self.__class__.__name__} , data: {data}"
                )
            )
        if not isinstance(parent_alti_key, str):
            raise InvalidParentKeyException(f"ParentKey {parent_alti_key} is not a str.")
        return parent_alti_key
