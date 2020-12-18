import datetime
from typing import Union

SCALAR_TYPES = (bool, int, float, str, datetime.datetime)

# NOTE: this order is important to Pydantic, see
# https://github.com/samuelcolvin/pydantic/issues/436 - especially
# that bool occurs before int; otherwise bools will be converted
# to ints where possible
Scalar = Union[bool, int, float, str, datetime.datetime]
