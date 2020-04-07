"""LogEvent for AWS related events."""
from dataclasses import dataclass

from altimeter.core.log import BaseLogEvent, EventName


@dataclass(frozen=True)
class AWSLogEvents(BaseLogEvent):
    """AWS specific Log event names"""

    AuthToAccountStart: EventName
    AuthToAccountEnd: EventName
    AuthToAccountFailure: EventName

    GetSubAccountsStart: EventName
    GetSubAccountsEnd: EventName

    RunAccountScanLambdaStart: EventName
    RunAccountScanLambdaEnd: EventName

    MuxerQueueScan: EventName
    MuxerStart: EventName
    MuxerEnd: EventName
    MuxerStat: EventName

    ScanAWSAccountsStart: EventName
    ScanAWSAccountsEnd: EventName

    ScanAWSAccountStart: EventName
    ScanAWSAccountEnd: EventName
    ScanAWSAccountError: EventName

    ScanAWSAccountServiceStart: EventName
    ScanAWSAccountServiceEnd: EventName

    ScanAWSResourcesNonFatalError: EventName

    ScanConfigured: EventName
