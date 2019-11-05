"""Lambda execution time exceptions."""
from altimeter.core.exceptions import AltimeterException


class RequiredEnvironmentVariableNotPresentException(AltimeterException):
    """An environment variable required by a lambda is not present."""


class RequiredEventVariableNotPresentException(AltimeterException):
    """An event variable required by a lambda is not present."""
