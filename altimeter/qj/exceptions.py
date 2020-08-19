"""QJ Exception Classes"""


class QJException(Exception):
    """Base exception class for all QJ thrown exceptions"""


class JobInvalid(QJException):
    """A specified Job is invalid"""


class JobNotFound(QJException):
    """A specified Job could not be found"""


class JobVersionNotFound(QJException):
    """A specified JobVersion could not be found"""


class ActiveJobVersionNotFound(QJException):
    """An active JobVersion for a specified Job could not be found"""


class JobQueryMissingAccountId(QJException):
    """A Job's query is missing the required account_id field"""


class JobQueryInvalid(QJException):
    """A Job's query is invalid SPARQL"""


class ResultSetNotFound(QJException):
    """A specified ResultSet could not be found"""


class ResultSetResultsLimitExceeded(QJException):
    """The number of Results in a ResultSet exceeds the configured maximum"""


class ResultSizeExceeded(QJException):
    """The size of an individual result exceeds the configured maximum"""
