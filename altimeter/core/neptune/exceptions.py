"""Neptune errors"""
from altimeter.core.exceptions import AltimeterException


class NeptuneClientException(AltimeterException):
    """Base exception class for Neptune client exceptions."""


class NeptuneQueryException(NeptuneClientException):
    """A server-side error occurred during a Neptune query execution."""


class NeptuneNoGraphsFoundException(NeptuneClientException):
    """No graphs were found in Neptune."""


class NeptuneNoFreshGraphFoundException(NeptuneClientException):
    """No acceptably recent graph could be found in Neptune."""


class NeptuneClearGraphException(NeptuneClientException):
    """An error occurred while clearing a graph."""


class NeptuneUpdateGraphException(NeptuneClientException):
    """An error occurred while updating a graph."""


class NeptuneLoadGraphException(NeptuneClientException):
    """An error occurred while loading a graph."""


class InvalidQueryException(NeptuneClientException):
    """A statically detected error with a query was found."""
