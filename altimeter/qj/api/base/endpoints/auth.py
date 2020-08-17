"""Endpoints for service auth"""
from typing import Any

from fastapi import APIRouter, Security

from altimeter.qj.api import deps

ROUTER = APIRouter()


@ROUTER.get("")
def get_auth(api_key: str = Security(deps.api_key),) -> Any:
    """Get the current auth token"""
    return api_key
