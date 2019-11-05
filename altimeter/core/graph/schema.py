"""A Schema consists of a list of Fields which define how to parse an arbitrary dictionary
into a list of Links."""
from typing import Any, Dict, List

from altimeter.core.graph.field.base import Field
from altimeter.core.graph.link.base import Link


class Schema:
    """A Schema consists of a list of Fields which define how to parse an arbitrary dictionary
    into a list of :class:`altimeter.core.graph.links.Link`.
    The schema method performs translation to :class:`altimeter.core.graph.links.Link`.

    Args:
        fields: fields for this Schema.
    """

    def __init__(self, *fields: Field) -> None:
        self.fields = fields

    def parse(self, data: Dict[str, Any], context: Dict[str, Any]) -> List[Link]:
        """Parse this schema into a list of Links

        Args:
            data: raw data to parse
            context: contains auxiliary information which can be passed through the parse process.

        Returns:
            A list of :class:`altimeter.core.graph.links.Link` .
        """
        links: List[Any] = []
        for field in self.fields:
            links += field.parse(data, context)
        return links
