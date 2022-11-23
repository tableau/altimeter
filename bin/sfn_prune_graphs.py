#!/usr/bin/env python3
"""Prune graphs from Neptune"""
import logging
from typing import Any, Dict

from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.config import GraphPrunerConfig
from altimeter.core.pruner import prune_graph


class PruneGraphsInput(BaseImmutableModel):
    config_path: str


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Lambda entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    prune_graphs_input = PruneGraphsInput(**event)
    pruner_config = GraphPrunerConfig(config_path=prune_graphs_input.config_path)
    prune_results = prune_graph(pruner_config)
    return prune_results.dict()
