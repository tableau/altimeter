"""Remediation schema"""
# pylint: disable=too-few-public-methods

from pydantic import BaseModel


class Remediation(BaseModel):
    """Remediation schema"""

    job_name: str
    result_set_id: str

    class Config:
        """Pydantic config overrides"""

        orm_mode = True
