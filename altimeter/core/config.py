"""Configuration classes"""
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Type, Tuple

import boto3
import toml

from altimeter.aws.auth.accessor import Accessor


class InvalidConfigException(Exception):
    """Indicates an invalid configuration"""


def get_required_param(key: str, config_dict: Dict[str, Any]) -> Any:
    """Get a parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist."""
    value = config_dict.get(key)
    if value is None:
        raise InvalidConfigException(f"Missing parameter '{key}'")
    return value


def get_required_list_param(key: str, config_dict: Dict[str, Any]) -> Tuple[Any, ...]:
    """Get a list parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a list. Return as a tuple."""
    value = get_required_param(key, config_dict)
    if not isinstance(value, list):
        raise InvalidConfigException(f"Parameter '{key}' should be a list. Is {type(value)}")
    return tuple(value)


def get_required_bool_param(key: str, config_dict: Dict[str, Any]) -> bool:
    """Get a bool parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a bool."""
    value = get_required_param(key, config_dict)
    if not isinstance(value, bool):
        raise InvalidConfigException(f"Parameter '{key}' should be a bool. Is {type(value)}")
    return value


def get_required_int_param(key: str, config_dict: Dict[str, Any]) -> int:
    """Get a int parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a int."""
    value = get_required_param(key, config_dict)
    if not isinstance(value, int):
        raise InvalidConfigException(f"Parameter '{key}' should be a int. Is {type(value)}")
    return value


def get_required_section(key: str, config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Get a section from a config dict. Raise InvalidConfigException if it does not exist."""
    value = config_dict.get(key)
    if value is None:
        raise InvalidConfigException(f"Missing section '{key}'")
    return value


@dataclass(frozen=True)
class AccessConfig:
    """Access configuration class"""

    accessor: Accessor

    @classmethod
    def from_dict(cls: Type["AccessConfig"], config_dict: Dict[str, Any]) -> "AccessConfig":
        """Build an AccessConfig from a dict"""
        cache_creds = get_required_bool_param("cache_creds", config_dict)
        accessor = Accessor.from_dict(config_dict, cache_creds=cache_creds)
        return AccessConfig(accessor=accessor)


@dataclass(frozen=True)
class ScanConfig:
    """Scan configuration class"""

    accounts: Tuple[str, ...]
    regions: Tuple[str, ...]
    scan_sub_accounts: bool
    preferred_account_scan_regions: Tuple[str, ...]
    single_account_mode: bool

    @classmethod
    def from_dict(cls: Type["ScanConfig"], config_dict: Dict[str, Any]) -> "ScanConfig":
        """Build a ScanConfig from a dict"""
        preferred_account_scan_regions = get_required_list_param(
            "preferred_account_scan_regions", config_dict
        )
        accounts = get_required_list_param("accounts", config_dict)
        regions = get_required_list_param("regions", config_dict)
        scan_sub_accounts = get_required_bool_param("scan_sub_accounts", config_dict)
        if accounts:
            single_account_mode = False
        else:
            single_account_mode = True
        if not accounts:
            sts_client = boto3.client("sts")
            account_id = sts_client.get_caller_identity()["Account"]
            accounts = (account_id,)

        return ScanConfig(
            accounts=accounts,
            regions=regions,
            scan_sub_accounts=scan_sub_accounts,
            preferred_account_scan_regions=preferred_account_scan_regions,
            single_account_mode=single_account_mode,
        )


@dataclass(frozen=True)
class ConcurrencyConfig:
    """Concurrency configuration class"""

    max_account_scan_threads: int
    max_accounts_per_thread: int
    max_svc_scan_threads: int

    @classmethod
    def from_dict(
        cls: Type["ConcurrencyConfig"], config_dict: Dict[str, Any]
    ) -> "ConcurrencyConfig":
        """Build a ConcurrencyConfig from a dict"""
        max_account_scan_threads = get_required_int_param("max_account_scan_threads", config_dict)
        max_accounts_per_thread = get_required_int_param("max_accounts_per_thread", config_dict)
        max_svc_scan_threads = get_required_int_param("max_svc_scan_threads", config_dict)
        return ConcurrencyConfig(
            max_account_scan_threads=max_account_scan_threads,
            max_accounts_per_thread=max_accounts_per_thread,
            max_svc_scan_threads=max_svc_scan_threads,
        )


@dataclass(frozen=True)
class Config:
    """Top level configuration class"""

    access: AccessConfig
    concurrency: ConcurrencyConfig
    scan: ScanConfig

    def __post_init__(self) -> None:
        if (
            self.scan.single_account_mode
            and not self.scan.scan_sub_accounts
            and self.access.accessor.multi_hop_accessors
        ):
            raise InvalidConfigException("Accessor config not supported for single account mode")

    @classmethod
    def from_dict(cls: Type["Config"], config_dict: Dict[str, Any]) -> "Config":
        """Build a Config from a dict"""
        scan_dict = get_required_section("scan", config_dict)
        try:
            scan = ScanConfig.from_dict(scan_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"{str(ice)} in section 'scan'")

        concurrency_dict = get_required_section("concurrency", config_dict)
        try:
            concurrency = ConcurrencyConfig.from_dict(concurrency_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"{str(ice)} in section 'concurrency'")

        access_dict = get_required_section("access", config_dict)
        try:
            access = AccessConfig.from_dict(access_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"{str(ice)} in section 'access'")
        return Config(access=access, concurrency=concurrency, scan=scan,)

    @classmethod
    def from_file(cls: Type["Config"], filepath: Path) -> "Config":
        """Load a Config from a file"""
        config_dict = dict(toml.load(filepath))
        try:
            return cls.from_dict(config_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"Error in conf file {filepath}: {str(ice)}")
