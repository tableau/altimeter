"""Misc RDF related code"""
from dataclasses import dataclass

from rdflib import Graph


@dataclass(frozen=True)
class GraphPackage:
    """Represents a rdflib.Graph and associated metadata."""

    graph: Graph
    name: str
    version: str
    start_time: int
    end_time: int
