# pylint: disable=too-few-public-methods
"""Pydantic Status schemas"""
from pydantic import BaseModel  # pylint: disable=no-name-in-module


class Status(BaseModel):
    """Status schema"""

    status: str
