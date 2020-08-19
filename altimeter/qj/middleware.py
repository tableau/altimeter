"""FastAPI middlewares"""
import time

from starlette.exceptions import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from altimeter.core.log import Logger
from altimeter.qj.log import QJLogEvents


class HTTPRequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware which performs HTTP request logging"""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Middleware dispatch func which logs requests"""
        start_time = time.time()
        error_detail = None
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except HTTPException as ex:
            status_code = ex.status_code
            error_detail = str(ex.detail)
        except Exception as ex:
            status_code = HTTP_500_INTERNAL_SERVER_ERROR
            error_detail = str(ex)
        finally:
            end_time = time.time()
            user_agent = request.headers.get("user-agent", "unknown")
            xff = request.headers.get("x-forwarded_for", None)
            log_fields = {
                "event": QJLogEvents.HTTPRequest,
                "requestor": request.client.host,
                "user-agent": user_agent,
                "url": str(request.url),
                "request_time": end_time - start_time,
                "status_code": status_code,
            }
            if error_detail is not None:
                log_fields["error"] = error_detail
            if xff is not None:
                log_fields["x-forwarded-for"] = xff
            logger = Logger()
            logger.info(**log_fields)
        return JSONResponse({"detail": error_detail}, status_code=status_code)
