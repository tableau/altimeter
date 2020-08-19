"""FastAPI dependencies"""
from typing import Generator

from botocore.exceptions import ClientError
from fastapi import Depends, HTTPException
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session
from starlette.status import HTTP_403_FORBIDDEN

from altimeter.core.log import Singleton
from altimeter.qj.config import APIServiceConfig, DBConfig
from altimeter.qj.crud.crud_job import CRUDJob
from altimeter.qj.crud.crud_result_set import CRUDResultSet
from altimeter.qj.security import get_api_key
from altimeter.qj.settings import API_KEY_HEADER_NAME


# pylint: disable=too-few-public-methods
class SessionGenerator(metaclass=Singleton):
    """Singleton class for generating db sesssions"""

    def __init__(self) -> None:
        db_config = DBConfig()
        self._engine = create_engine(db_config.get_db_uri(), pool_pre_ping=True, pool_recycle=3600)

    def get_session(self) -> Session:
        """Get a db session object"""
        return Session(autocommit=False, autoflush=False, bind=self._engine)


def db_session() -> Generator[Session, None, None]:
    """Get a db session"""
    try:
        session = SessionGenerator().get_session()
        yield session
    finally:
        session.close()


def job_crud() -> CRUDJob:
    """Get a CRUDJob object"""
    api_svc_config = APIServiceConfig()
    return CRUDJob(
        db_ro_user=api_svc_config.db_ro_user,
        result_expiration_sec_default=api_svc_config.result_expiration_sec_default,
        result_expiration_sec_limit=api_svc_config.result_expiration_sec_limit,
        max_graph_age_sec_default=api_svc_config.max_graph_age_sec_default,
        max_graph_age_sec_limit=api_svc_config.max_graph_age_sec_limit,
        max_result_age_sec_default=api_svc_config.max_result_age_sec_default,
        max_result_age_sec_limit=api_svc_config.max_result_age_sec_limit,
        account_id_key=api_svc_config.account_id_key,
    )


def result_set_crud() -> CRUDResultSet:
    """Get a CRUDResultSet object"""
    api_svc_config = APIServiceConfig()
    return CRUDResultSet(
        max_result_set_results=api_svc_config.max_result_set_results,
        max_result_size_bytes=api_svc_config.max_result_size_bytes,
        job_crud=job_crud(),
    )


def api_key(key: str = Depends(APIKeyHeader(name=API_KEY_HEADER_NAME))) -> str:
    """Validate an api key string matches the value currently in SecretsManager"""
    region = APIServiceConfig().region
    current_api_key_secret = get_api_key(region_name=region)
    if key == current_api_key_secret:
        return key
    try:
        pending_api_key_secret = get_api_key(version_stage="AWSPENDING", region_name=region)
        if key == pending_api_key_secret:
            return key
    except ClientError as c_e:
        response_error = getattr(c_e, "response", {}).get("Error", {})
        error_code = response_error.get("Code", "")
        if error_code != "ResourceNotFoundException":
            raise c_e
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials")
