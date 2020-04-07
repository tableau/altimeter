"""A GraphSet represents the contents of a Graph."""
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Type

from rdflib import BNode, Graph, Literal, Namespace, RDF

from altimeter.core.graph.exceptions import (
    GraphSetOrphanedReferencesException,
    UnmergableGraphSetsException,
)
from altimeter.core.graph.link.links import ResourceLinkLink
from altimeter.core.graph.node_cache import NodeCache
from altimeter.core.multilevel_counter import MultilevelCounter
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec


class GraphSet:
    """A GraphSet represents the contents of a Graph.  It contains a list of Resource objects, a
    graph name and version and a few metadata fields describing the scan. Generally GraphSets
    are created by running `GraphSpec.scan`.

    Args:
        name: graph name
        version: graph version
        start_time: epoch scan start time
        end_time: epoch scan end time
        resources: Resource objects
        errors: Errors encountered during scan
        stats: Scan statistics, generally API call stats.
    """

    def __init__(
        self,
        name: str,
        version: str,
        start_time: int,
        end_time: int,
        resources: List[Resource],
        errors: List[str],
        stats: MultilevelCounter,
    ):
        self.name = name
        self.version = version
        self.start_time = start_time
        self.end_time = end_time
        self.resources = resources
        self.errors = errors
        self.stats = stats
        self._resolve_duplicates()

    def to_rdf(self) -> Graph:
        """Generate an rdflib.Graph from this GraphSet.

        Returns:
            rdf.Graph object representing this GraphSet.
        """
        namespace = Namespace(f"{self.name}:")
        node_cache = NodeCache()
        graph = Graph()
        metadata_node = BNode()
        graph.add((metadata_node, RDF.type, getattr(namespace, "metadata")))
        graph.add((metadata_node, getattr(namespace, "name"), Literal(self.name)))
        graph.add((metadata_node, getattr(namespace, "version"), Literal(self.version)))
        graph.add((metadata_node, getattr(namespace, "start_time"), Literal(self.start_time)))
        graph.add((metadata_node, getattr(namespace, "end_time"), Literal(self.end_time)))
        for error in self.errors:
            graph.add((metadata_node, getattr(namespace, "error"), Literal(error)))
        for resource in self.resources:
            resource.to_rdf(namespace=namespace, graph=graph, node_cache=node_cache)
        return graph

    def to_dict(self) -> Dict[str, Any]:
        """Generate a dictionary representation of this GraphSet.

        Returns:
            dict representation of this GraphSet
        """
        resources = {resource.resource_id: resource.to_dict() for resource in self.resources}
        return {
            "name": self.name,
            "version": self.version,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "resources": resources,
            "errors": self.errors,
            "stats": self.stats.to_dict(),
        }

    def validate(self) -> None:
        """Validate that all inter-resource relationships in this GraphSet resolve.

        Raises:
            GraphSetOrphanedReferencesException if references to Resources are found in
                the graph which do not exist. For example, if an EC2 instance has a VPC and
                references it as a ResourceLinkField if that VPC is not found as a Resource in the
                graph this exception is raised.
        """
        present_resource_ids = {resource.resource_id for resource in self.resources}
        resource_ref_ids_used_by_ids: DefaultDict[str, List[str]] = defaultdict(list)
        for resource in self.resources:
            for link in resource.links:
                if isinstance(link, ResourceLinkLink):
                    resource_ref_ids_used_by_ids[link.obj].append(resource.resource_id)
        resource_ref_ids = set(resource_ref_ids_used_by_ids.keys())
        orphan_refs = resource_ref_ids - present_resource_ids
        if orphan_refs:
            raise GraphSetOrphanedReferencesException(
                ("References to resources were found which were not scanned: " f"{orphan_refs}.")
            )

    def _resolve_duplicates(self) -> None:
        """Resolve any duplicate resource ids.  In general duplicate resource ids can
        have their Resource objects merged if they are of the same type and all fields
        are identical or additive only across the resources or if one of the Resources
        allows a special merge via its ResourceSpec class' `allow_clobber` attribute."""
        resource_ids_resources: DefaultDict[str, List[Resource]] = defaultdict(list)
        for resource in self.resources:
            resource_ids_resources[resource.resource_id].append(resource)
        merged_resources: List[Resource] = []
        for resource_id, resources in resource_ids_resources.items():
            if len(resources) > 1:
                merged_resource = ResourceSpec.merge_resources(
                    resource_id=resource_id, resources=resources
                )
                merged_resources.append(merged_resource)
        for merged_resource in merged_resources:
            self.resources = [
                resource
                for resource in self.resources
                if resource.resource_id != merged_resource.resource_id
            ]
            self.resources.append(merged_resource)

    @classmethod
    def from_dict(cls: Type["GraphSet"], data: Dict[str, Any]) -> "GraphSet":
        """Create a GraphSet from a dict.

        Args:
            data: dict of Resource data

        Returns:
            GraphSet object
        """
        resources: List[Resource] = []
        name = data["name"]
        start_time = data["start_time"]
        end_time = data["end_time"]
        version = data["version"]
        errors = data["errors"]
        stats = MultilevelCounter.from_dict(data["stats"])
        for resource_id, resource_data in data["resources"].items():
            resource = Resource.from_dict(resource_id, resource_data)
            resources.append(resource)
        return cls(
            name=name,
            version=version,
            start_time=start_time,
            end_time=end_time,
            resources=resources,
            errors=errors,
            stats=stats,
        )

    def merge(self, other: "GraphSet") -> None:
        """Merge another GraphSet into this GraphSet.

        Args:
            other: GraphSet to merge into this GraphSet.
        """
        if other.name != self.name:
            raise UnmergableGraphSetsException(
                f"Unable to merge graph with name {other.name} into {self.name}"
            )
        if other.version != self.version:
            raise UnmergableGraphSetsException(
                f"Unable to merge graph with version {other.version} into {self.version}"
            )
        self.start_time = min(self.start_time, other.start_time)
        self.end_time = max(self.end_time, other.end_time)
        self.resources += other.resources
        self._resolve_duplicates()
        self.errors += other.errors
        self.stats.merge(other.stats)
