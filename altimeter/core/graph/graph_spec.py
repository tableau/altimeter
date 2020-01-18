"""A GraphSpec contains a specification to scan and create a graph."""
import time
from typing import Any, Dict, List, Tuple, Type

from altimeter.core.log import LogEvent, Logger

from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.multilevel_counter import MultilevelCounter
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
        self._resource_spec_types_classes: Dict[str, Type[ResourceSpec]] = {
            resource_spec_class.type_name: resource_spec_class
            for resource_spec_class in self.resource_spec_classes
        }

    def scan(self) -> GraphSet:
        """Perform a scan on all of the resource classes in this GraphSpec and return
        a GraphSet containing the scanned data.

        Returns:
            GraphSet representing results of scanning this GraphSpec's resource_spec_classes.
        """

        resources: List[Resource] = []
        errors: List[str] = []
        start_time = int(time.time())
        logger = Logger()
        for resource_spec_class in self.resource_spec_classes:
            with logger.bind(resource_type=str(resource_spec_class.type_name)):
                logger.debug(event=LogEvent.ScanResourceTypeStart)
                resource_scan_result = resource_spec_class.scan(scan_accessor=self.scan_accessor)
                resources += resource_scan_result.resources
                errors += resource_scan_result.errors
                logger.debug(event=LogEvent.ScanResourceTypeEnd)
        end_time = int(time.time())
        return GraphSet(
            name=self.name,
            version=self.version,
            start_time=start_time,
            end_time=end_time,
            resources=resources,
            errors=errors,
            stats=self.scan_accessor.api_call_stats,
        )
