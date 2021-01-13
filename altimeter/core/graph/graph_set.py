"""A GraphSet represents the contents of a Graph."""
from collections import defaultdict
import json
import itertools
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Tuple, Type
import uuid

from pydantic import validator
from rdflib import BNode, Graph, Literal, Namespace, RDF

from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.graph.exceptions import (
    GraphSetOrphanedReferencesException,
    UnmergableGraphSetsException,
)
from altimeter.core.graph.exceptions import DuplicateResourceIdsFoundException
from altimeter.core.graph.node_cache import NodeCache
from altimeter.core.resource.resource import Resource
from altimeter.core.resource.resource_spec import ResourceSpec


class GraphSet(BaseImmutableModel):
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
    """

    name: str
    version: str
    start_time: int
    end_time: int
    resources: Tuple[Resource, ...]
    errors: List[str]

    @classmethod
    def from_json_file(cls: Type["GraphSet"], path: Path) -> "GraphSet":
        with path.open("r") as json_fp:
            json_data = json.load(json_fp)
            return cls.parse_obj(json_data)

    @classmethod
    def from_graph_sets(cls, graph_sets: List["GraphSet"]) -> "GraphSet":
        """Create a new GraphSet from a list of GraphSets"""
        names = {graph_set.name for graph_set in graph_sets}
        if len(names) != 1:
            raise UnmergableGraphSetsException(
                f"Unable to merge graphs with differing names {names}"
            )
        name = names.pop()
        versions = {graph_set.version for graph_set in graph_sets}
        if len(versions) != 1:
            raise UnmergableGraphSetsException(
                f"Unable to merge graphs with differing versions {versions}"
            )
        version = versions.pop()
        start_time = min([graph_set.start_time for graph_set in graph_sets])
        end_time = max([graph_set.end_time for graph_set in graph_sets])
        resources = list(
            itertools.chain.from_iterable([graph_set.resources for graph_set in graph_sets])
        )
        errors = list(itertools.chain.from_iterable([graph_set.errors for graph_set in graph_sets]))
        return cls(
            name=name,
            version=version,
            start_time=start_time,
            end_time=end_time,
            resources=resources,
            errors=errors,
        )


class ValidatedGraphSet(GraphSet):
    """A GraphSet which has been validated - duplicate resources have been merged and
    required resource links have been verified"""

    # pylint: disable=no-self-argument,no-self-use
    @validator("resources")
    def dedupe_and_validate_resources(cls, resources: Tuple[Resource, ...]) -> Tuple[Resource, ...]:
        deduped_resources = dedupe_resources(resources)
        validate_resources(deduped_resources)
        return deduped_resources

    def to_rdf(self) -> Graph:
        """Generate an rdflib.Graph from this ValidatedGraphSet.

        Returns:
            rdf.Graph object representing this ValidatedGraphSet.
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

    @classmethod
    def from_graph_set(cls, graph_set: GraphSet) -> "ValidatedGraphSet":
        """Create a ValidatedGraphSet from a GraphSet"""
        return cls(
            name=graph_set.name,
            version=graph_set.version,
            start_time=graph_set.start_time,
            end_time=graph_set.end_time,
            resources=graph_set.resources,
            errors=graph_set.errors,
        )

    def to_neptune_lpg(self, scan_id: str) -> Dict:
        vertices = []
        edges = []
        vertex = {
            "~id": scan_id,
            "~label": "metadata",
            "name": self.name,
            "version": self.version,
            "start_time": self.start_time,
            "end_time": self.end_time,
        }
        vertices.append(vertex)
        for error in self.errors:
            vertex = {"~id": str(uuid.uuid1()), "~label": "error", "error": error}
            vertices.append(vertex)
            edges.append(
                {
                    "~id": str(uuid.uuid1()),
                    "~label": "generated",
                    "~from": f"{self.name}:{self.version}",
                    "~to": vertex["~id"],
                }
            )
            vertices.append(vertex)
        for resource in self.resources:
            resource.to_lpg(vertices, edges)

        for v in vertices:
            # Add the scan_id parameter to each vertex
            v["scan_id"] = scan_id
            # Add an edge from each vertex to the metadata vertex
            edges.append(
                {
                    "~id": uuid.uuid1(),
                    "~label": "identified_resource",
                    "~from": scan_id,
                    "~to": v["~id"],
                }
            )
        return {"vertices": vertices, "edges": edges}


def dedupe_resources(resources: Iterable[Resource]) -> Tuple[Resource, ...]:
    """Resolve any duplicate resource ids.  In general duplicate resource ids can
    have their Resource objects merged if they are of the same type and all fields
    are identical or additive only across the resources or if one of the Resources
    allows a special merge via its ResourceSpec class' `allow_clobber` attribute."""
    resource_ids_resources: DefaultDict[str, List[Resource]] = defaultdict(list)
    for resource in resources:
        resource_ids_resources[resource.resource_id].append(resource)
    merged_resources: List[Resource] = []
    for resource_id, candidate_resources in resource_ids_resources.items():
        if len(candidate_resources) > 1:
            merged_resource = ResourceSpec.merge_resources(
                resource_id=resource_id, resources=candidate_resources
            )
            merged_resources.append(merged_resource)
    for merged_resource in merged_resources:
        resource_ids_resources[merged_resource.resource_id] = [merged_resource]
    # at this point all values in resource_ids_resources should be single-value lists.
    # validate that and build a list of deduped_resources from those single-values
    deduped_resources = []
    for resource_id, resource_list in resource_ids_resources.items():
        if len(resource_list) != 1:
            raise Exception(f"More than one resource found for {resource_id}: {resource_list}")
        deduped_resources.append(resource_list[0])
    return tuple(deduped_resources)


def validate_resources(resources: Tuple[Resource, ...]) -> None:
    """Validate that all inter-resource relationships in a tuple of Resources are valid and
    that there are no duplicate resource ids.

    Raises:
        GraphSetOrphanedReferencesException if references to Resources are found in
            the graph which do not exist. For example, if an EC2 instance has a VPC and
            references it as a ResourceLinkField if that VPC is not found as a Resource in the
            graph this exception is raised.
        DuplicateResourceIdsFoundException if duplicate resource ids are found
    """
    present_resource_ids = {resource.resource_id for resource in resources}
    if len(present_resource_ids) != len(resources):
        all_resource_ids = sorted([resource.resource_id for resource in resources])
        raise DuplicateResourceIdsFoundException(
            f"Found duplicate resources ids in {all_resource_ids}"
        )
    resource_ref_ids_used_by_ids: DefaultDict[str, List[str]] = defaultdict(list)
    for resource in resources:
        if resource.link_collection.resource_links:
            for link in resource.link_collection.resource_links:
                resource_ref_ids_used_by_ids[link.obj].append(resource.resource_id)
    resource_ref_ids = set(resource_ref_ids_used_by_ids.keys())
    orphan_refs = resource_ref_ids - present_resource_ids
    if orphan_refs:
        raise GraphSetOrphanedReferencesException(
            ("References to resources were found which were not scanned: " f"{orphan_refs}.")
        )
