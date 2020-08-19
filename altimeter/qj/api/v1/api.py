"""V1 API router"""
from fastapi import APIRouter

from altimeter.qj.api.v1.endpoints.jobs import JOBS_ROUTER
from altimeter.qj.api.v1.endpoints.result_sets import RESULT_SETS_ROUTER

V1_ROUTER = APIRouter()
V1_ROUTER.include_router(JOBS_ROUTER, prefix="/jobs", tags=["jobs"])
V1_ROUTER.include_router(RESULT_SETS_ROUTER, prefix="/result_sets", tags=["result_sets"])
