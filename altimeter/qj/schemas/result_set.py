"""ResultSet and Result schemas"""
# pylint: disable=too-few-public-methods
from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel, Field, validator  # pylint: disable=no-name-in-module

from altimeter.qj.schemas.job import Job


class Result(BaseModel):
    """ResultBase schema"""

    account_id: str
    result: Dict[str, Any]

    # pylint: disable=no-self-argument,no-self-use
    @validator("account_id")
    def zero_pad_account_id(cls, value: str) -> str:
        """Zero pad aws account ids"""
        return value.zfill(12)

    # pylint: disable=no-self-argument,no-self-use
    @validator("account_id")
    def account_id_is_int(cls, value: str) -> str:
        """Validate that an account id is an integer"""
        try:
            int(value)
        except ValueError as v_e:
            raise ValueError(f"account_id {value} is not an integer: {v_e}")
        return value

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"
        orm_mode = True


class ResultSetGraphSpec(BaseModel):
    """ResultSetGraphSpec schema"""

    graph_uris_load_times: Dict[str, int]


class ResultSetBase(BaseModel):
    """ResultSetBase schema"""

    job: Job
    graph_spec: ResultSetGraphSpec
    results: List[Result]
    created: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"


class ResultSetCreate(ResultSetBase):
    """ResultSetCreate schema"""

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"


class ResultSet(ResultSetBase):
    """ResultSet schema"""

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"
        orm_mode = True


class ResultSetsPruneResult(BaseModel):
    """ResultSetsPruneResult schema"""

    num_pruned: int

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"
