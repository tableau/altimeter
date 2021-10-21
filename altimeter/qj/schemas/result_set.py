"""ResultSet and Result schemas"""
# pylint: disable=too-few-public-methods
import io
import csv

from enum import Enum
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator  # pylint: disable=no-name-in-module

from altimeter.qj.schemas.job import Job


class Result(BaseModel):
    """ResultBase schema"""

    account_id: str
    result: Dict[str, Any]

    # pylint: disable=no-self-argument,no-self-use
    @validator("account_id")
    def zero_pad_account_id(cls, value: str) -> str:
        """Zero pad aws account ids"""
        return value.zfill(12)

    # pylint: disable=no-self-argument,no-self-use
    @validator("account_id")
    def account_id_is_int(cls, value: str) -> str:
        """Validate that an account id is an integer"""
        try:
            int(value)
        except ValueError as v_e:
            raise ValueError(f"account_id {value} is not an integer: {v_e}") from v_e
        return value

    class Config:
        """Pydantic config overrides"""

        orm_mode = True


class ResultSetGraphSpec(BaseModel):
    """ResultSetGraphSpec schema"""

    graph_uris_load_times: Dict[str, int]


class ResultSetBase(BaseModel):
    """ResultSetBase schema"""

    job: Job
    graph_spec: ResultSetGraphSpec
    results: List[Result]
    created: datetime = Field(default_factory=datetime.utcnow)


class ResultSetCreate(ResultSetBase):
    """ResultSetCreate schema"""


class ResultSet(ResultSetBase):
    """ResultSet schema"""

    result_set_id: str

    # pylint: disable=no-self-argument,no-self-use
    @validator("result_set_id", pre=True)
    def stringify_result_set_id(cls, value: str) -> str:
        """Stringify the result_set_id"""
        return str(value)

    class Config:
        """Pydantic config overrides"""

        orm_mode = True

    def to_csv(self) -> str:
        """Create a CSV representation of the ResultSet.
        """
        with io.StringIO() as csv_buf:
            if self.results:
                fieldnames = tuple(self._flatten_result(self.results[0]).keys())
                writer: csv.DictWriter = csv.DictWriter(
                    csv_buf, fieldnames=fieldnames, lineterminator="\n"
                )
                writer.writeheader()
                for result in self.results:
                    writer.writerow(self._flatten_result(result))
            csv_buf.seek(0)
            return csv_buf.read()

    def _flatten_result(self, result: Result) -> Dict[str, Any]:
        """Flattens Result object into single-level dict"""
        result_data = result.dict()
        flattened_result = {**result_data, **result_data["result"]}
        del flattened_result["result"]
        return flattened_result


class ResultSetsPruneResult(BaseModel):
    """ResultSetsPruneResult schema"""

    num_pruned: int


# pylint: disable=too-few-public-methods
class ResultSetFormat(str, Enum):
    """Format options for ResultSet"""

    json = "json"
    csv = "csv"
