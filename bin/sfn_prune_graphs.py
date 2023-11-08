#!/usr/bin/env python3
"""Prune graphs from Neptune"""
import logging
from typing import Any, Dict

from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.config import Config
from altimeter.core.pruner import prune_graph_from_config


class PruneGraphsInput(BaseImmutableModel):
    config: Config


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Lambda entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    prune_graphs_input = PruneGraphsInput(**event)
    prune_results = prune_graph_from_config(prune_graphs_input.config)
    return prune_results.dict()
