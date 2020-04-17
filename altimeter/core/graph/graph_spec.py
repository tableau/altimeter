"""A GraphSpec contains a specification to scan and create a graph."""
from typing import Any, List, Tuple, Type

from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent

from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec


class GraphSpec:
    """A GraphSpec contains a specification to scan and create a graph. It contains a set of
    ResourceSpec classes defining what to scan and a scan_accessor object defining how to scan.

    Args:
        name: graph name
        version: graph version
        resource_spec_classes: tuple of ResourceSpec classes
        scan_accessor: object which is passed to ResourceSpec.scan for each ResourceSpec in
                       resource_spec_classes. It should provide methods which ResourceSpec.scan
                       can use to access whatever API the ResourceSpec needs to access.
    """

    def __init__(
        self,
        name: str,
        version: str,
        resource_spec_classes: Tuple[Type[ResourceSpec], ...],
        scan_accessor: Any,
    ):
        self.name = name
        self.version = version
        self.resource_spec_classes = resource_spec_classes
        self.scan_accessor = scan_accessor

    def scan(self) -> List[Resource]:
        """Perform a scan on all of the resource classes in this GraphSpec and return
        a list of Resource objects.

        Returns:
            List of Resource objects
        """
        resources: List[Resource] = []
        logger = Logger()
        for resource_spec_class in self.resource_spec_classes:
            with logger.bind(resource_type=str(resource_spec_class.type_name)):
                logger.debug(event=LogEvent.ScanResourceTypeStart)
                scanned_resources = resource_spec_class.scan(scan_accessor=self.scan_accessor)
                resources += scanned_resources
                logger.debug(event=LogEvent.ScanResourceTypeEnd)
        return resources
