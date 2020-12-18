"""Base pydantic altimeter model classes"""
from pydantic import BaseModel


class BaseImmutableModel(BaseModel):
    """Base immutable pydantic altimeter model"""

    class Config:
        """Pydantic config"""

        allow_mutation = False
        extra = "forbid"
        arbitrary_types_allowed = True


class BaseMutableModel(BaseModel):
    """Base mutable pydantic altimeter model"""

    class Config:
        """Pydantic config"""

        extra = "forbid"
        arbitrary_types_allowed = True
