"""LogEvent for QJ events."""
from dataclasses import dataclass
import logging

from altimeter.core.log import BaseLogEvent, EventName

# Clear handlers
_ROOT = logging.getLogger()
for handler in _ROOT.handlers:
    _ROOT.removeHandler(handler)


@dataclass(frozen=True)
class QJLogEvents(BaseLogEvent):
    """QJ Log event names"""

    # pylint: disable=invalid-name

    # General
    InitConfig: EventName

    # Executor
    GetJobs: EventName
    ScheduleJob: EventName

    # Pruner
    DeleteStart: EventName
    DeleteEnd: EventName

    # Query
    InitJob: EventName
    RunQueryStart: EventName
    RunQueryEnd: EventName
    CreateResultSetStart: EventName
    CreateResultSetEnd: EventName

    # Notifications
    NotifyNewResultsStart: EventName
    NotifyNewResultsEnd: EventName

    # CRUD Jobs
    CreateJob: EventName
    DeleteJob: EventName
    GetActiveJob: EventName
    GetJobVersion: EventName
    GetJobVersions: EventName
    UpdateJob: EventName
    CreateView: EventName
    DropView: EventName

    # CRUD Results
    CreateResultSet: EventName
    DeleteExpiredResultSets: EventName
    GetExpiredResultSets: EventName
    GetResultSet: EventName
    GetLatestResultSetForActiveJob: EventName

    # Remediations
    ProcessResult: EventName
    RemediationInit: EventName
    StaleResultSet: EventName
    JobHasNoRemediator: EventName
    InvokeResultRemediationLambdaStart: EventName
    InvokeResultRemediationLambdaEnd: EventName
    InvokeResultRemediationLambdaError: EventName
    ResultRemediationLambdaRunError: EventName
    ResultRemediationStart: EventName
    ResultRemediationFailed: EventName
    ResultRemediationSuccessful: EventName
    ResultSetRemediationFailed: EventName
    ResultSetRemediationSuccessful: EventName

    # HTTP
    APIError: EventName
    HTTPRequest: EventName
