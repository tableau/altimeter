from dataclasses import dataclass

from altimeter.core.log import BaseLogEvent, EventName


@dataclass(frozen=True)
class LogEvent(BaseLogEvent):
    """Contains EventNames for logging."""

    AuthToAccountStart: EventName
    AuthToAccountEnd: EventName
    AuthToAccountFailure: EventName

    GraphLoadedSNSNotificationStart: EventName
    GraphLoadedSNSNotificationEnd: EventName

    MetadataGraphUpdateStart: EventName
    MetadataGraphUpdateEnd: EventName

    NeptuneLoadStart: EventName
    NeptuneLoadEnd: EventName
    NeptuneLoadPolling: EventName
    NeptuneLoadError: EventName

    PruneNeptuneGraphStart: EventName
    PruneNeptuneGraphEnd: EventName
    PruneNeptuneGraphError: EventName
    PruneNeptuneGraphSkip: EventName
    PruneOrphanedNeptuneGraphStart: EventName
    PruneOrphanedNeptuneGraphEnd: EventName

    PruneNeptuneGraphsStart: EventName
    PruneNeptuneGraphsEnd: EventName
    PruneNeptuneGraphsError: EventName

    ReadFromFSStart: EventName
    ReadFromFSEnd: EventName

    ReadFromS3Start: EventName
    ReadFromS3End: EventName

    ScanResourceTypeStart: EventName
    ScanResourceTypeEnd: EventName

    WriteToFSStart: EventName
    WriteToFSEnd: EventName

    WriteToS3Start: EventName
    WriteToS3End: EventName
