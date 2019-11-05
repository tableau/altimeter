"""Field parsing related exceptions."""
from altimeter.core.exceptions import AltimeterException


class ScalarFieldSourceKeyNotFoundException(AltimeterException):
    """The source_key of a ScalarField is not found"""


class ScalarFieldValueNotAScalarException(AltimeterException):
    """The value of a ScalarField is not a string, bool, int or float."""


class ResourceLinkFieldSourceKeyNotFoundException(AltimeterException):
    """The source_key of a ResourceLinkField is not found"""


class ResourceLinkFieldValueNotAStringException(AltimeterException):
    """The value of a ResourceLinkField is not a string."""


class TagsFieldMissingTagsKeyException(AltimeterException):
    """A TagsField data is missing key 'Tags'"""


class ParentKeyMissingException(AltimeterException):
    """A required ParentKey is missing."""


class InvalidParentKeyException(AltimeterException):
    """A ParentKey is invalid."""


class DictFieldValueNotADictException(AltimeterException):
    """A DictField does not contain a dict."""


class DictFieldSourceKeyNotFoundException(AltimeterException):
    """The source_key of a DictField was not found"""


class DictFieldValueIsNullException(AltimeterException):
    """The value of a non-nullable DictField was null"""
