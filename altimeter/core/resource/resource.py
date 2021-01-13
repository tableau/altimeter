from typing import Dict, List, Union

from rdflib import Graph, Literal, Namespace, RDF, URIRef

from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.graph.links import LinkCollection
from altimeter.core.graph.node_cache import NodeCache


class Resource(BaseImmutableModel):
    """A Resource defines a single scanned resource which is directly translatable to a graph
    node.  It contains an id, type name and list of Links.

    Args:
         resource_id: id of this resource
         type: type name of this resource
         link_collection: a LinkCollection representing links from this resource
    """

    resource_id: str
    type: str
    link_collection: LinkCollection

    def to_rdf(self, namespace: Namespace, graph: Graph, node_cache: NodeCache) -> None:
        """Graph this Resource as a URIRef on a Graph.

        Args:
            namespace: RDF namespace to use for predicates and objects when graphing
                       this resource's links
            graph: RDF graph
            node_cache: NodeCache to use for any cached URIRef lookups
        """
        node = node_cache.setdefault(self.resource_id, URIRef(self.resource_id))
        graph.add((node, RDF.type, getattr(namespace, self.type)))
        graph.add((node, getattr(namespace, "id"), Literal(self.resource_id)))
        self.link_collection.to_rdf(
            subj=node, namespace=namespace, graph=graph, node_cache=node_cache
        )

    def to_lpg(self, vertices: List[Dict], edges: List[Dict]) -> None:
        """Graph this Resource as a dictionary into the vertices and edges lists.

        Args:
            vertices: List containing dictionaries representing a vertex
            edges: List containing dictionaries representing an edge
        """
        vertex = {
            "~id": self.resource_id,
            "~label": self.type,
            "arn": self.resource_id,
        }
        self.link_collection.to_lpg(vertex, vertices, edges)
        vertices.append(vertex)
