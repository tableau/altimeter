"""Job schemas"""
# pylint: disable=too-few-public-methods
from datetime import datetime
from enum import Enum
import re
from typing import List, Optional

from pydantic import BaseModel, Field, validator  # pylint: disable=no-name-in-module


class Category(str, Enum):
    """Job Category"""

    gov: str = "gov"
    sec: str = "sec"


class Severity(str, Enum):
    """Job Severity"""

    debug: str = "debug"
    info: str = "info"
    warn: str = "warn"
    error: str = "error"


class JobGraphSpec(BaseModel):
    """JobGraphSpec schema"""

    graph_names: List[str]


class JobBase(BaseModel):
    """JobBase schema"""

    name: str
    description: str
    graph_spec: JobGraphSpec
    category: Category
    severity: Severity
    query: str

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"

    # pylint: disable=no-self-argument,no-self-use
    @validator("name")
    def name_is_valid(cls, value: str) -> str:
        """Validate the name of a job. Must begin with a letter and may consist of one or more
        alphanumerics or underscores."""
        if not re.match(r"^[a-z][a-z0-9_]*$", value, re.IGNORECASE):
            raise ValueError(
                f"Job name {value} is not valid. Jobs must begin with a letter and may contain "
                "letters, numbers and underscores"
            )
        return value


class JobCreate(JobBase):
    """JobCreate schema"""

    max_graph_age_sec: Optional[int] = Field(gt=0)
    result_expiration_sec: Optional[int] = Field(gt=0)
    max_result_age_sec: Optional[int] = Field(gt=0)

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"


class JobUpdate(BaseModel):
    """JobUpdate schema. Contains fields that can be updated. Some fields
    like query can not be updated as they will fundamentally change the result schema view"""

    active: Optional[bool]
    description: Optional[str]
    category: Optional[Category]
    severity: Optional[Severity]
    max_graph_age_sec: Optional[int] = Field(gt=0)
    result_expiration_sec: Optional[int] = Field(gt=0)
    max_result_age_sec: Optional[int] = Field(gt=0)

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"

    @classmethod
    def from_job_create(cls, job_create: JobCreate) -> "JobUpdate":
        """Build a JobUpdate object from the relevant fields of a JobCreate object"""

        class JobUpdateIntermediate(JobUpdate):
            """Intermediate schema used to build a JobCreate from a JobUpdate"""

            class Config:
                """Pydantic config overrides"""

                extra = "allow"
                orm_mode = True

        intermediate = JobUpdateIntermediate.from_orm(job_create)
        return cls(**dict(intermediate))


class Job(JobBase):
    """Job schema"""

    active: bool = False
    created: datetime = Field(default_factory=datetime.utcnow)
    query_fields: List[str]
    max_graph_age_sec: int = Field(..., gt=0)
    result_expiration_sec: int = Field(..., gt=0)
    max_result_age_sec: int = Field(..., gt=0)

    class Config:
        """Pydantic config overrides"""

        extra = "forbid"
        orm_mode = True
