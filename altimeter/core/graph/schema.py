"""A Schema consists of a list of Fields which define how to parse an arbitrary dictionary
into a list of Links."""
from typing import Any, Dict

from altimeter.core.graph.field.base import Field
from altimeter.core.graph.links import LinkCollection


class Schema:
    """A Schema consists of a list of Fields which define how to parse an arbitrary dictionary
    into a :class:`altimeter.core.graph.links.LinkCollection`.

    Args:
        fields: fields for this Schema.
    """

    def __init__(self, *fields: Field) -> None:
        self.fields = fields

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> LinkCollection:
        """Parse this schema into a list of Links

        Args:
            data: raw data to parse
            context: contains auxiliary information which can be passed through the parse process.

        Returns:
            LinkCollection
        """
        link_collection = LinkCollection()
        for field in self.fields:
            link_collection += field.parse(data, context)
        return link_collection
