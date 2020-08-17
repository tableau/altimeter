"""Base API router"""
from fastapi import APIRouter

from altimeter.qj.api.base.endpoints import auth, status

BASE_ROUTER = APIRouter()
BASE_ROUTER.include_router(auth.ROUTER, prefix="/auth", tags=["auth"])
BASE_ROUTER.include_router(status.ROUTER, prefix="/status", tags=["status"])
