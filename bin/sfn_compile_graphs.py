#!/usr/bin/env python3
"""CompileGraphs StepFunction Lambda
Given a list of AccountScanManifests create complete json and rdf files in S3."""
import logging
from typing import Any, Dict, List, Set, Tuple

from altimeter.aws.scan.account_scan_manifest import AccountScanManifest
from altimeter.aws.scan.scan_manifest import ScanManifest
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import GZIP, ArtifactWriter
from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.config import AWSConfig
from altimeter.core.graph.graph_set import GraphSet, ValidatedGraphSet


class CompileGraphsInput(BaseImmutableModel):
    config: AWSConfig
    scan_id: str
    account_scan_manifests: Tuple[AccountScanManifest, ...]


class CompileGraphsOutput(BaseImmutableModel):
    rdf_path: str


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Lambda entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    compile_graphs_input = CompileGraphsInput(**event)

    scanned_accounts: List[str] = []
    artifacts: List[str] = []
    errors: Dict[str, List[str]] = {}
    unscanned_accounts: Set[str] = set()
    graph_sets: List[GraphSet] = []

    artifact_reader = ArtifactReader.from_artifact_path(compile_graphs_input.config.artifact_path)
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=compile_graphs_input.config.artifact_path,
        scan_id=compile_graphs_input.scan_id,
    )

    for account_scan_manifest in compile_graphs_input.account_scan_manifests:
        account_id = account_scan_manifest.account_id
        if account_scan_manifest.errors:
            errors[account_id] = account_scan_manifest.errors
            unscanned_accounts.add(account_id)
        if account_scan_manifest.artifacts:
            for account_scan_artifact in account_scan_manifest.artifacts:
                artifacts.append(account_scan_artifact)
                artifact_graph_set_dict = artifact_reader.read_json(account_scan_artifact)
                graph_sets.append(GraphSet.parse_obj(artifact_graph_set_dict))
            scanned_accounts.append(account_id)
        else:
            unscanned_accounts.add(account_id)
    if not graph_sets:
        raise Exception("BUG: No graph_sets generated.")
    validated_graph_set = ValidatedGraphSet.from_graph_set(GraphSet.from_graph_sets(graph_sets))
    master_artifact_path = artifact_writer.write_json(name="master", data=validated_graph_set)
    start_time = validated_graph_set.start_time
    end_time = validated_graph_set.end_time
    scan_manifest = ScanManifest(
        scanned_accounts=scanned_accounts,
        master_artifact=master_artifact_path,
        artifacts=artifacts,
        errors=errors,
        unscanned_accounts=list(unscanned_accounts),
        start_time=start_time,
        end_time=end_time,
    )
    artifact_writer.write_json("manifest", data=scan_manifest)
    rdf_path = artifact_writer.write_graph_set(
        name="master", graph_set=validated_graph_set, compression=GZIP
    )

    return CompileGraphsOutput(rdf_path=rdf_path,).dict()
