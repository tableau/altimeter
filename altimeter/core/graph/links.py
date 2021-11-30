"""A Link represents the predicate-object portion of a triple."""
import abc
import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union

from pydantic import validator
from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef, XSD

from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.graph import SCALAR_TYPES
from altimeter.core.graph.node_cache import NodeCache


class BaseLink(BaseImmutableModel, abc.ABC):
    """A link represents the predicate-object portion of a triple.
    BaseLink is an abstract base class for Link subclasses.
    """

    def to_rdf(
        self, subj: BNode, namespace: Namespace, graph: Graph, node_cache: NodeCache
    ) -> None:
        """Graph this link on a BNode in a Graph using a given Namespace to create the full
        predicate.

        Args:
             subj: subject portion of triple - graph this link's pred, obj against it.
             namespace: RDF namespace to use for this triple's predicate
             graph: RDF graph
             node_cache: NodeCache to use to find cached nodes.
        """

    def to_lpg(self, parent: Dict, vertices: List[Dict], edges: List[Dict], prefix: str) -> None:
        """Graph this link on a BNode in a Graph using a given Namespace to create the full
        predicate.

        Args:
             parent: a dictionary og the parent
             vertices: a list of dictionaries of the vertices for a labeled property graph
             edges: a list of dictionaries of the edges for a labeled property graph
             prefix: a prefix to add to the attribute name
        """


class SimpleLink(BaseLink):
    """A SimpleLink represents a scalar value. In RDF terms a SimpleLink creates a Literal
    in the graph."""

    pred: str
    obj: Any

    # pylint: disable=no-self-argument,no-self-use
    @validator("obj")
    def obj_is_scalar(cls, val: Any) -> Any:
        if not isinstance(val, SCALAR_TYPES):
            raise ValueError(
                (f"Expected data to be one of {SCALAR_TYPES}, is " f"{type(val)}: {val}")
            )
        return val

    def to_rdf(
        self, subj: BNode, namespace: Namespace, graph: Graph, node_cache: NodeCache
    ) -> None:
        """Graph this link on a BNode in a Graph using a given Namespace to create the full
        predicate.

        Args:
             subj: subject portion of triple - graph this link's pred, obj against it.
             namespace: RDF namespace to use for this triple's predicate
             graph: RDF graph
             node_cache: NodeCache to use to find cached nodes.
        """
        datatype = None
        if isinstance(self.obj, int):
            if self.obj > 2147483647:
                datatype = XSD.nonNegativeInteger
        literal = Literal(self.obj, datatype=datatype)
        graph.add((subj, getattr(namespace, self.pred), literal))

    def to_lpg(
        self, parent: Dict, vertices: List[Dict], edges: List[Dict], prefix: str = ""
    ) -> None:
        """Convert this link to the appropriate vertices, edges, and properties

        Args:
             :parent: the parent dictionary vertex
             :param vertices: the list of all vertex dictionaries
             :param edges: the list of all edge dictionaries
             :param prefix: the prefix assigned to the key
             :type parent: Dict
        """
        obj = self.obj
        if isinstance(obj, int):
            # Need to handle numbers that are bigger than a Long in Java, for now we stringify it
            if obj > 9223372036854775807 or obj < -9223372036854775807:
                obj = str(obj)
        elif isinstance(obj, SimpleLink):
            print("ERROR ERROR")
        parent[prefix + self.pred] = obj


class MultiLink(BaseLink):
    """Represents a named set of sublinks.  For example an 'EBSVolumeAttachemnt'
    MultiLink could exist which specifies sublinks Volume, AttachTime"""

    pred: str
    obj: "LinkCollection"

    def to_rdf(
        self, subj: BNode, namespace: Namespace, graph: Graph, node_cache: NodeCache
    ) -> None:
        """Graph this link on a BNode in a Graph using a given Namespace to create the full
        predicate.

        Args:
             subj: subject portion of triple - graph this link's pred, obj against it.
             namespace: RDF namespace to use for this triple's predicate
             graph: RDF graph
             node_cache: NodeCache to use to find cached nodes.
        """
        map_node = BNode()
        graph.add((map_node, RDF.type, getattr(namespace, f"{self.pred}")))
        self.obj.to_rdf(map_node, namespace, graph, node_cache)
        graph.add((subj, getattr(namespace, self.pred), map_node))

    def to_lpg(
        self, parent: Dict, vertices: List[Dict], edges: List[Dict], prefix: str = ""
    ) -> None:
        """Convert this link to the appropriate vertices, edges, and properties

        Args:
             :parent: the parent dictionary vertex
             vertices: the list of all vertex dictionaries
             edges: the list of all edge dictionaries
             prefix: A string to prefix the property name with
        """
        vertex_id = uuid.uuid1()
        v = {
            "~id": vertex_id,
            "~label": self.pred,
        }
        edge_label = prefix if prefix != "" else self.pred
        edge = {
            "~id": uuid.uuid1(),
            "~label": edge_label,
            "~from": parent["~id"],
            "~to": vertex_id,
        }
        edges.append(edge)
        vertices.append(v)
        self.obj.to_lpg(v, vertices, edges)


class ResourceLink(BaseLink):
    """Represents a link to another resource which must exist in the graph."""

    pred: str
    obj: str

    def to_rdf(
        self, subj: BNode, namespace: Namespace, graph: Graph, node_cache: NodeCache
    ) -> None:
        """Graph this link on a BNode in a Graph using a given Namespace to create the full
        predicate.

        Args:
             subj: subject portion of triple - graph this link's pred, obj against it.
             namespace: RDF namespace to use for this triple's predicate
             graph: RDF graph
             node_cache: NodeCache to use to find cached nodes.
        """
        link_node = node_cache.setdefault(self.obj, URIRef(self.obj))
        graph.add((subj, getattr(namespace, self.pred), link_node))

    def to_lpg(
        self, parent: Dict, vertices: List[Dict], edges: List[Dict], prefix: str = ""
    ) -> None:
        """Convert this link to the appropriate vertices, edges, and properties

        Args:
             :parent: the parent dictionary vertex
             vertices: the list of all vertex dictionaries
             edges: the list of all edge dictionaries
             prefix: string to prefix the property name with
        """
        edge = {
            "~id": uuid.uuid1(),
            "~label": "resource_link",
            "~from": parent["~id"],
            "~to": self.obj,
        }
        edges.append(edge)


class TransientResourceLink(BaseLink):
    """Represents a link to another resource which may or may not exist in the graph."""

    pred: str
    obj: str

    def to_rdf(
        self, subj: BNode, namespace: Namespace, graph: Graph, node_cache: NodeCache
    ) -> None:
        """Graph this link on a BNode in a Graph using a given Namespace to create the full
        predicate.

        Args:
             subj: subject portion of triple - graph this link's pred, obj against it.
             namespace: RDF namespace to use for this triple's predicate
             graph: RDF graph
             node_cache: NodeCache to use to find cached nodes.
        """
        link_node = node_cache.setdefault(self.obj, URIRef(self.obj))
        graph.add((subj, getattr(namespace, self.pred), link_node))

    def to_lpg(
        self, parent: Dict, vertices: List[Dict], edges: List[Dict], prefix: str = ""
    ) -> None:
        """Convert this link to the appropriate vertices, edges, and properties

        Args:
             :parent: the parent dictionary vertex
             vertices: the list of all vertex dictionaries
             edges: the list of all edge dictionaries
             prefix: string to prefix the property name with
        """
        edge = {
            "~id": uuid.uuid1(),
            "~label": "transient_resource_link",
            "~from": parent["~id"],
            "~to": self.obj,
        }
        edges.append(edge)


class TagLink(BaseLink):
    """Represents a AWS-style Tag attached to a node."""

    pred: str
    obj: str

    def to_rdf(
        self, subj: BNode, namespace: Namespace, graph: Graph, node_cache: NodeCache
    ) -> None:
        """Graph this link on a BNode in a Graph using a given Namespace to create the full
        predicate.

        Args:
             subj: subject portion of triple - graph this link's pred, obj against it.
             namespace: RDF namespace to use for this triple's predicate
             graph: RDF graph
             node_cache: NodeCache to use to find cached nodes.
        """
        tag_id = f"{self.pred}:{self.obj}"
        tag_node = node_cache.get(tag_id)
        if tag_node is None:
            tag_node = BNode()
            graph.add((tag_node, namespace.key, Literal(self.pred)))
            graph.add((tag_node, namespace.value, Literal(self.obj)))
            graph.add((tag_node, RDF.type, getattr(namespace, "tag")))
            node_cache[tag_id] = tag_node
        graph.add((subj, getattr(namespace, "tag"), tag_node))

    def to_lpg(
        self, parent: Dict, vertices: List[Dict], edges: List[Dict], prefix: str = ""
    ) -> None:
        """Convert this link to the appropriate vertices, edges, and properties

        Args:
             :parent:git  the parent dictionary vertex
             vertices: the list of all vertex dictionaries
             edges: the list of all edge dictionaries
             prefix: string to prefix the property name with
        """
        if not any(x["~id"] == f"{self.pred}:{self.obj}" for x in vertices):
            vertex = {}
            vertex["~id"] = f"{self.pred}:{self.obj}"
            vertex["~label"] = "tag"
            vertex[self.pred] = self.obj
            vertices.append(vertex)
        edge = {
            "~id": uuid.uuid1(),
            "~label": "tagged",
            "~from": parent["~id"],
            "~to": f"{self.pred}:{self.obj}",
        }
        edges.append(edge)


Link = Union[SimpleLink, MultiLink, TagLink, ResourceLink, TransientResourceLink]


class LinkCollection(BaseImmutableModel):
    simple_links: Optional[Tuple[SimpleLink, ...]] = None
    multi_links: Optional[Tuple[MultiLink, ...]] = None
    tag_links: Optional[Tuple[TagLink, ...]] = None
    resource_links: Optional[Tuple[ResourceLink, ...]] = None
    transient_resource_links: Optional[Tuple[TransientResourceLink, ...]] = None

    def to_rdf(
        self, subj: BNode, namespace: Namespace, graph: Graph, node_cache: NodeCache
    ) -> None:
        """Graph this LinkCollection on an RDF graph"""
        if self.simple_links:
            for simple_link in self.simple_links:
                simple_link.to_rdf(
                    subj=subj, namespace=namespace, graph=graph, node_cache=node_cache
                )
        if self.multi_links:
            for multi_link in self.multi_links:
                multi_link.to_rdf(
                    subj=subj, namespace=namespace, graph=graph, node_cache=node_cache
                )
        if self.tag_links:
            for tag_link in self.tag_links:
                tag_link.to_rdf(subj=subj, namespace=namespace, graph=graph, node_cache=node_cache)
        if self.resource_links:
            for resource_link in self.resource_links:
                resource_link.to_rdf(
                    subj=subj, namespace=namespace, graph=graph, node_cache=node_cache
                )
        if self.transient_resource_links:
            for transient_resource_link in self.transient_resource_links:
                transient_resource_link.to_rdf(
                    subj=subj, namespace=namespace, graph=graph, node_cache=node_cache
                )

    def to_lpg(
        self, vertex: Dict[str, Any], vertices: List[Dict], edges: List[Dict], prefix: str = ""
    ) -> None:
        """Graph this LinkCollection as a labelled property graph"""
        if self.simple_links:
            for simple_link in self.simple_links:
                simple_link.to_lpg(vertex, vertices, edges, prefix)
        if self.multi_links:
            for multi_link in self.multi_links:
                multi_link.to_lpg(vertex, vertices, edges, prefix)
        if self.tag_links:
            for tag_link in self.tag_links:
                tag_link.to_lpg(vertex, vertices, edges, prefix)
        if self.resource_links:
            for resource_link in self.resource_links:
                resource_link.to_lpg(vertex, vertices, edges, prefix)
        if self.transient_resource_links:
            for transient_resource_link in self.transient_resource_links:
                transient_resource_link.to_lpg(vertex, vertices, edges, prefix)

    def get_links(self) -> Tuple[Link, ...]:
        return (
            (self.simple_links if self.simple_links else ())
            + (self.multi_links if self.multi_links else ())
            + (self.tag_links if self.tag_links else ())
            + (self.resource_links if self.resource_links else ())
            + (self.transient_resource_links if self.transient_resource_links else ())
        )

    @classmethod
    def from_links(cls: Type["LinkCollection"], links: Iterable[Link]) -> "LinkCollection":
        simple_links: List[SimpleLink] = []
        multi_links: List[MultiLink] = []
        tag_links: List[TagLink] = []
        resource_links: List[ResourceLink] = []
        transient_resource_links: List[TransientResourceLink] = []

        for link in links:
            if isinstance(link, SimpleLink):
                simple_links.append(link)
            elif isinstance(link, MultiLink):
                multi_links.append(link)
            elif isinstance(link, TagLink):
                tag_links.append(link)
            elif isinstance(link, ResourceLink):
                resource_links.append(link)
            elif isinstance(link, TransientResourceLink):
                transient_resource_links.append(link)

        args = {
            "simple_links": simple_links,
            "multi_links": multi_links,
            "tag_links": tag_links,
            "resource_links": resource_links,
            "transient_resource_links": transient_resource_links,
        }
        args_without_nulls = {key: val for key, val in args.items() if val}
        return cls(**args_without_nulls)

    def __add__(self, other: "LinkCollection") -> "LinkCollection":
        simple_links = (self.simple_links if self.simple_links else ()) + (
            other.simple_links if other.simple_links else ()
        )
        multi_links = (self.multi_links if self.multi_links else ()) + (
            other.multi_links if other.multi_links else ()
        )
        tag_links = (self.tag_links if self.tag_links else ()) + (
            other.tag_links if other.tag_links else ()
        )
        resource_links = (self.resource_links if self.resource_links else ()) + (
            other.resource_links if other.resource_links else ()
        )
        transient_resource_links = (
            self.transient_resource_links if self.transient_resource_links else ()
        ) + (other.transient_resource_links if other.transient_resource_links else ())
        args = {
            "simple_links": simple_links,
            "multi_links": multi_links,
            "tag_links": tag_links,
            "resource_links": resource_links,
            "transient_resource_links": transient_resource_links,
        }
        args_without_nulls = {key: val for key, val in args.items() if val}
        return LinkCollection(**args_without_nulls)


MultiLink.update_forward_refs()
