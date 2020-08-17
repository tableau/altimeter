"""Entrypoint"""
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
import uvicorn

from altimeter.core.log import Logger
from altimeter.qj.api.base.api import BASE_ROUTER
from altimeter.qj.api.v1.api import V1_ROUTER
from altimeter.qj.config import APIServiceConfig
from altimeter.qj.log import QJLogEvents
from altimeter.qj.middleware import HTTPRequestLoggingMiddleware

API_SVC_CONFIG = APIServiceConfig()

LOGGER = Logger()

app = FastAPI(title=API_SVC_CONFIG.app_name,)

app.include_router(BASE_ROUTER)
app.include_router(V1_ROUTER, prefix="/v1")
app.add_middleware(HTTPRequestLoggingMiddleware)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, ex: HTTPException):
    LOGGER.info(
        event=QJLogEvents.APIError,
        detail=ex.detail,
        url=str(request.url),
        requestor=request.client.host,
    )
    return JSONResponse(status_code=ex.status_code, content={"detail": ex.detail})


if __name__ == "__main__":
    uvicorn.run(app, host=API_SVC_CONFIG.api_host, port=API_SVC_CONFIG.api_port)
