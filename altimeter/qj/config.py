"""Global Settings"""
from typing import Any, Dict

from pydantic import BaseSettings, root_validator


from altimeter.qj.settings import (
    DEFAULT_RESULT_EXPIRATION_SEC_DEFAULT,
    DEFAULT_RESULT_EXPIRATION_SEC_LIMIT,
    DEFAULT_MAX_GRAPH_AGE_SEC_DEFAULT,
    DEFAULT_MAX_GRAPH_AGE_SEC_LIMIT,
    DEFAULT_MAX_RESULT_AGE_SEC_DEFAULT,
    DEFAULT_MAX_RESULT_AGE_SEC_LIMIT,
)


# pylint: disable=too-few-public-methods
class QJConfig(BaseSettings):
    """General high level configuration"""

    app_name: str = "QueryJobExecutor"
    account_id_key: str = "account_id"
    region: str


# pylint: disable=too-few-public-methods
class SecurityConfig(BaseSettings):
    """Security related configuration"""

    api_key_secret_name: str


# pylint: disable=too-few-public-methods
class APIConfig(QJConfig):
    """Configuraton for the API service and consumers"""

    api_host: str
    api_port: int


# pylint: disable=too-few-public-methods
class PrunerConfig(APIConfig, SecurityConfig):
    """Configuraton for the Pruner lambda"""


# pylint: disable=too-few-public-methods
class ExecutorConfig(APIConfig):
    """Configuraton for the Executor lambda"""

    query_queue_url: str


# pylint: disable=too-few-public-methods
class QueryConfig(APIConfig, SecurityConfig):
    """Configuraton for the Query lambda"""

    neptune_port: int
    neptune_host: str
    neptune_region: str


# pylint: disable=too-few-public-methods
class APIKeyRotatorConfig(APIConfig):
    """Configuraton for the APIKeyRotator lambda"""


# pylint: disable=too-few-public-methods
class DBConfig(BaseSettings):
    """Configuraton for the DB"""

    db_user: str
    db_password: str
    db_host: str
    db_name: str

    def get_db_uri(self) -> str:
        """Get a db uri for this DBConfig"""
        return f"postgres://{self.db_user}:{self.db_password}@{self.db_host}/{self.db_name}"


# pylint: disable=too-few-public-methods
class LimitsConfig(BaseSettings):
    """Limit configurations"""

    # protections
    max_result_set_results: int = 10000
    max_result_size_bytes: int = 4096

    # job defaults
    result_expiration_sec_default: int = DEFAULT_RESULT_EXPIRATION_SEC_DEFAULT
    result_expiration_sec_limit: int = DEFAULT_RESULT_EXPIRATION_SEC_LIMIT

    max_graph_age_sec_default: int = DEFAULT_MAX_GRAPH_AGE_SEC_DEFAULT
    max_graph_age_sec_limit: int = DEFAULT_MAX_GRAPH_AGE_SEC_LIMIT

    max_result_age_sec_default: int = DEFAULT_MAX_RESULT_AGE_SEC_DEFAULT
    max_result_age_sec_limit: int = DEFAULT_MAX_RESULT_AGE_SEC_LIMIT


# pylint: disable=too-few-public-methods
class APIServiceConfig(APIConfig, DBConfig, LimitsConfig, SecurityConfig):
    """Configuration for the API service"""

    # db extras
    db_ro_user: str

    # pylint: disable=no-self-argument,no-self-use
    @root_validator(pre=True)
    def check_result_expiration_sec(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that result_expiration_sec_default does not exceed
         result_expiration_sec_limit"""
        result_expiration_sec_default = values.get(
            "result_expiration_sec_default", DEFAULT_RESULT_EXPIRATION_SEC_DEFAULT
        )
        result_expiration_sec_limit = values.get(
            "result_expiration_sec_limit", DEFAULT_RESULT_EXPIRATION_SEC_LIMIT
        )
        if result_expiration_sec_default > result_expiration_sec_limit:
            raise ValueError(
                f"result_expiration_sec_default value {result_expiration_sec_default} is larger "
                f"than result_expiration_sec_limit value {result_expiration_sec_limit}"
            )
        return values

    # pylint: disable=no-self-argument,no-self-use
    @root_validator(pre=True)
    def check_max_graph_age_sec(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that max_graph_age_sec_default does not exceed
         max_graph_age_sec_limit"""
        max_graph_age_sec_default = values.get(
            "max_graph_age_sec_default", DEFAULT_MAX_GRAPH_AGE_SEC_DEFAULT
        )
        max_graph_age_sec_limit = values.get(
            "max_graph_age_sec_limit", DEFAULT_MAX_GRAPH_AGE_SEC_LIMIT
        )
        if max_graph_age_sec_default > max_graph_age_sec_limit:
            raise ValueError(
                f"max_graph_age_sec_default value {max_graph_age_sec_default} is larger "
                f"than max_graph_age_sec_limit value {max_graph_age_sec_limit}"
            )
        return values

    # pylint: disable=no-self-argument,no-self-use
    @root_validator(pre=True)
    def check_max_result_age_sec(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that max_result_age_sec_default does not exceed
         max_result_age_sec_limit"""
        max_result_age_sec_default = values.get(
            "max_result_age_sec_default", DEFAULT_MAX_RESULT_AGE_SEC_DEFAULT
        )
        max_result_age_sec_limit = values.get(
            "max_result_age_sec_limit", DEFAULT_MAX_RESULT_AGE_SEC_LIMIT
        )
        if max_result_age_sec_default > max_result_age_sec_limit:
            raise ValueError(
                f"max_result_age_sec_default value {max_result_age_sec_default} is larger "
                f"than max_result_age_sec_limit value {max_result_age_sec_limit}"
            )
        return values
