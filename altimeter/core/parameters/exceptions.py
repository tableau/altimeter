"""Lambda execution time exceptions."""
from altimeter.core.exceptions import AltimeterException


class RequiredVariableNotPresentException(AltimeterException):
    """A required variable is not present"""


class RequiredEnvironmentVariableNotPresentException(AltimeterException):
    """A required environment variable is not present."""


class RequiredEventVariableNotPresentException(AltimeterException):
    """An required lambda event variable is not present."""
