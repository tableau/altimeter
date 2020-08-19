"""Endpoints for service status"""
from typing import Any

from fastapi import APIRouter

from altimeter.qj import schemas

ROUTER = APIRouter()


@ROUTER.get("", response_model=schemas.Status)
def read_status() -> Any:
    """Get application status"""
    return schemas.Status(status="ok")
