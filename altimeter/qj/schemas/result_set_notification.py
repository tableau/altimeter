# pylint: disable=too-few-public-methods
"""Pydantic Notification schemas"""
from datetime import datetime

from pydantic import BaseModel  # pylint: disable=no-name-in-module

from altimeter.qj.schemas.job import Job
from altimeter.qj.schemas.result_set import ResultSetGraphSpec


class ResultSetNotification(BaseModel):
    """ResultSetNotification schema"""

    job: Job
    graph_spec: ResultSetGraphSpec
    created: datetime
    num_results: int
