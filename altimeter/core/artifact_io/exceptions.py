"""Exceptions for artifact I/O"""
from altimeter.core.exceptions import AltimeterException


class InvalidS3URIException(AltimeterException):
    """An S3 uri could not be parsed."""
