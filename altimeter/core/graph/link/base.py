import abc
import inspect
from typing import Any, Dict

from rdflib import BNode, Namespace, Graph

from altimeter.core.graph.node_cache import NodeCache


class Link(abc.ABC):
    """A Link represents the predicate-object portion of a triple.
    Links in general have a field_type which describes the nature of the relationship.

    Args:
        pred: predicate portion of the triple this Link represents
        obj: object portion of the triple this Link represents.
    """

    field_type: str = ""

    def __init__(self, pred: str, obj: Any):
        self.pred = pred
        self.obj = obj

    def __init_subclass__(cls, **kwargs: Any) -> None:
        if not inspect.isabstract(cls):
            for required in ("field_type",):
                if not getattr(cls, required):
                    raise TypeError(
                        f"Can not instantiate {cls.__name__} without {required} attribute."
                    )
        return super().__init_subclass__()

    def to_dict(self) -> Dict[str, str]:
        """Return a dictionary representation of this Link

        Returns:
            Dict representation of this Link
        """
        return {"pred": self.pred, "obj": self.obj, "type": self.field_type}

    @classmethod
    def from_dict(cls, pred: str, obj: Any) -> Any:
        """Create a Link object from a dict

        Args:
            pred: Link predicate
            obj: Link object

        Returns:
            Link subclass object
        """
        return cls(pred=pred, obj=obj)

    @abc.abstractmethod
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
