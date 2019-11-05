"""Exceptions related ton Graphing"""
from altimeter.core.exceptions import AltimeterException


class LinkParseException(AltimeterException):
    """Error parsing Links from JSON."""


class SchemaParseException(AltimeterException):
    """Schema.parse error"""


class EmbeddedFieldSourceKeyNotFoundException(AltimeterException):
    """The source_key of an EmbeddedField was not found"""


class EmbeddedFieldSourceKeyValueIsNoneException(AltimeterException):
    """A required source_key had a value but it was None."""


class MultiFieldSourceKeyNotFoundException(AltimeterException):
    """The source_key of a MultiField was not found"""


class MultiFieldValueNotAListException(AltimeterException):
    """A MultiField does not contain a List"""


class ResourceLinkFieldSourceKeyValueNotFoundException(AltimeterException):
    """The source_key of a ResourceLinkField was not found or had a None/0 value"""


class UnmergableDuplicateResourceIdsFoundException(AltimeterException):
    """Duplicate resource ids were found in a GraphSet and are not mergable"""


class ListFieldSourceKeyNotFoundException(AltimeterException):
    """The source_key of a ListField was not found"""


class ListFieldValueNotAListException(AltimeterException):
    """A ListField does not contain a list"""


class GraphSetOrphanedReferencesException(AltimeterException):
    """A GraphSet contained orphaned references, for instance a ResourceLink referring to
    an id not present in the GraphSet."""


class UnmergableGraphSetsException(AltimeterException):
    """GraphSets are unable to be merged."""
