#!/usr/bin/env python3
"""Convert Altimeter intermediate json to hyper format"""
from datetime import datetime
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys
from typing import DefaultDict, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Type
from types import MappingProxyType # TODO

# LEFT OFF - since Tableau doesn't support multiple relationships, I think
# we have to create join tables for all fk relationships :(

import tableauhyperapi

from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.link.base import Link
from altimeter.core.graph.link.links import SimpleLink, ResourceLinkLink, TagLink, MultiLink, TransientResourceLinkLink
from altimeter.core.resource.resource import Resource

def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json_filepaths", type=Path, nargs="+")
    args_ns = parser.parse_args(argv)

    input_json_filepaths = args_ns.input_json_filepaths
    scan_ids_graph_sets: Dict[int, GraphSet] = {scan_id: GraphSet.from_json_file(filepath) for scan_id, filepath in enumerate(input_json_filepaths)}

    with tableauhyperapi.HyperProcess(
            telemetry=tableauhyperapi.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with tableauhyperapi.Connection(endpoint=hyper.endpoint, database=f"altimeter.hyper",
                                        create_mode=tableauhyperapi.CreateMode.CREATE_AND_REPLACE) as connection:
            scan_table = tableauhyperapi.TableDefinition(
                table_name="scans",
                columns=[
                    tableauhyperapi.TableDefinition.Column("id", tableauhyperapi.SqlType.int(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                    tableauhyperapi.TableDefinition.Column("name", tableauhyperapi.SqlType.text(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                    tableauhyperapi.TableDefinition.Column("version", tableauhyperapi.SqlType.text(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                    tableauhyperapi.TableDefinition.Column("start_time", tableauhyperapi.SqlType.timestamp(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                    tableauhyperapi.TableDefinition.Column("end_time", tableauhyperapi.SqlType.timestamp(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),

                ]
            )
            connection.catalog.create_table(scan_table)
            # insert scan data
            for scan_id, graph_set in scan_ids_graph_sets.items():
                with tableauhyperapi.Inserter(connection, scan_table) as scan_inserter:
                    data = [(
                        scan_id,
                        graph_set.name,
                        graph_set.version,
                        datetime.fromtimestamp(graph_set.start_time),
                        datetime.fromtimestamp(graph_set.end_time),
                    )]
                    scan_inserter.add_rows(data)
                    scan_inserter.execute()

            tags_table = tableauhyperapi.TableDefinition(
                table_name="tags",
                columns=[
                    tableauhyperapi.TableDefinition.Column("scan_id", tableauhyperapi.SqlType.int(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                    tableauhyperapi.TableDefinition.Column("resource_id", tableauhyperapi.SqlType.text(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                    tableauhyperapi.TableDefinition.Column("key", tableauhyperapi.SqlType.text(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                    tableauhyperapi.TableDefinition.Column("value", tableauhyperapi.SqlType.text(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),

                ],
            )
            connection.catalog.create_table(tags_table)

            # first discover the relationships we need to model by iterating over all resources
            # in all provided graph sets
            type_names: Set[str] = set()
            types_simple_preds: DefaultDict[str, Set[str]] = defaultdict(set)
            types_resource_preds: DefaultDict[str, Set[str]] = defaultdict(set)
            types_transient_resource_preds: DefaultDict[str, Set[str]] = defaultdict(set)
            scan_ids_types_resources: DefaultDict[int, DefaultDict[str, List[Resource]]] = defaultdict(lambda: defaultdict(list))
            join_table_type_names__predicates: DefaultDict[Tuple[str, str], Set[str]] = defaultdict(set)
            tags_data: List[Tuple[str, str, str]] = []
            for scan_id, graph_set in scan_ids_graph_sets.items():
                for resource in graph_set.resources:
                    type_name = normalize_name(resource.type_name)
                    type_names.add(type_name)
                    for link in resource.links:
                        if isinstance(link, TagLink):
                            tags_data.append((scan_id, resource.resource_id, link.pred, link.obj))
                        elif isinstance(link, ResourceLinkLink):
                            types_resource_preds[type_name].add(link.pred)
                        elif isinstance(link, SimpleLink):
                            types_simple_preds[type_name].add(link.pred)
                        elif isinstance(link, MultiLink):
                            record_multilink_relationships(join_table_type_names__predicates, link, type_name)
                        elif isinstance(link, TransientResourceLinkLink):
                            types_transient_resource_preds[type_name].add(link.pred)
                        else:
                            raise NotImplementedError(f"Link type {type(link)} not supported")
                    scan_ids_types_resources[scan_id][type_name].append(resource)
            with tableauhyperapi.Inserter(connection, tags_table) as tags_inserter:
                tags_inserter.add_rows(tags_data)
                tags_inserter.execute()

            # process MultiLinks
            for (type_name, target_type_name), preds in join_table_type_names__predicates.items():
                build_multilink_tables_and_data(type_name, target_type_name, preds, scan_ids_types_resources, connection)

            # create entity tables
            for type_name in type_names:
                columns = []
                columns.append(
                    tableauhyperapi.TableDefinition.Column("scan_id", tableauhyperapi.SqlType.int(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE),
                )
                # id column
                columns.append(
                    tableauhyperapi.TableDefinition.Column(f"{type_name}_arn", tableauhyperapi.SqlType.text(),
                                                           nullability=tableauhyperapi.NOT_NULLABLE)
                )
                # SimpleLink columns
                unnormalized_simple_column_names: List[str] = []
                for simple_pred in types_simple_preds.get(type_name, set()):
                    simple_pred_type = get_pred_obj_type(simple_pred, type_name, scan_ids_types_resources)
                    if simple_pred_type in (bool, int, str):
                        unnormalized_simple_column_names.append(simple_pred)
                        simple_pred_name = normalize_name(simple_pred)
                        if simple_pred_type == str:
                            column = tableauhyperapi.TableDefinition.Column(simple_pred_name, tableauhyperapi.SqlType.text())
                        elif simple_pred_type == int:
                            column = tableauhyperapi.TableDefinition.Column(simple_pred_name, tableauhyperapi.SqlType.big_int())
                        elif simple_pred_type == bool:
                            column = tableauhyperapi.TableDefinition.Column(simple_pred_name, tableauhyperapi.SqlType.bool())
                        else:
                            raise Exception(f"I don't know how to handle simple_pred_type {simple_pred_type}")
                        columns.append(column)
                # ResourceLinkLink columns
                unnormalized_resource_link_column_names: List[str] = []
                for resource_pred in types_resource_preds.get(type_name, set()):
                    resource_pred_type = get_pred_obj_type(resource_pred, type_name, scan_ids_types_resources)
                    if resource_pred_type != str:
                        raise Exception(f"I don't know how to handle resource_pred_type {resource_pred_type}")
                    unnormalized_resource_link_column_names.append(resource_pred)
                    resource_pred_name = f"{normalize_name(resource_pred)}_arn"
                    column = tableauhyperapi.TableDefinition.Column(resource_pred_name, tableauhyperapi.SqlType.text())
                    columns.append(column)
                # TransientResourceLinkLink columns
                unnormalized_transient_resource_link_column_names: List[str] = []
                for transient_resource_pred in types_transient_resource_preds.get(type_name, set()):
                    transient_resource_pred_type = get_pred_obj_type(transient_resource_pred, type_name, scan_ids_types_resources)
                    if transient_resource_pred_type != str:
                        raise Exception(f"I don't know how to handle transient_resource_pred_type {transient_resource_pred_type}")
                    unnormalized_transient_resource_link_column_names.append(transient_resource_pred)
                    transient_resource_pred_name = f"{normalize_name(transient_resource_pred)}_arn"
                    column = tableauhyperapi.TableDefinition.Column(transient_resource_pred_name,
                                                                    tableauhyperapi.SqlType.text())
                    columns.append(column)
                # create the table
                table = tableauhyperapi.TableDefinition(
                    table_name=type_name,
                    columns=columns,
                )
                connection.catalog.create_table(table)

                # insert data
                with tableauhyperapi.Inserter(connection, table) as inserter:
                    data = []
                    for scan_id, types_resources in scan_ids_types_resources.items():
                        for resource in types_resources[type_name]:
                            resource_data = [scan_id, resource.resource_id]
                            # add SimpleLink data
                            for unnormalized_column_name in unnormalized_simple_column_names:
                                found = False
                                for link in resource.links:
                                    if link.pred == unnormalized_column_name:
                                        if isinstance(link.obj, bool):
                                            resource_data.append(link.obj)
                                        else:
                                            resource_data.append(link.obj)
                                        found = True
                                        break
                                if not found:
                                    resource_data.append(None)
                            # add ResourceLink data
                            for unnormalized_column_name in unnormalized_resource_link_column_names:
                                found = False
                                for link in resource.links:
                                    if link.pred == unnormalized_column_name:
                                        resource_data.append(link.obj)
                                        found = True
                                        break
                                if not found:
                                    resource_data.append(None)
                            # add TransientResourceLink data
                            for unnormalized_column_name in unnormalized_transient_resource_link_column_names:
                                found = False
                                for link in resource.links:
                                    if link.pred == unnormalized_column_name:
                                        resource_data.append(link.obj)
                                        found = True
                                        break
                                if not found:
                                    resource_data.append(None)
                            data.append(tuple(resource_data))
                    inserter.add_rows(data)
                    inserter.execute()
    return 0

def record_multilink_relationships(join_table_type_names__predicates: DefaultDict[Tuple[str, str], Set[str]], link: Link, type_name: str):
    # LEFT OFF HERE:
    # I think what we are missing is that a MultiLink should be represented by a table with a synthetic
    # id of some kind. That way child multilinks can refer to the parent.
    for obj in link.obj:
        if type(obj) in (ResourceLinkLink, TransientResourceLinkLink):
            normalized_pred = f"{obj.pred}_arn"
        elif isinstance(obj, SimpleLink):
            normalized_pred = obj.pred
        elif isinstance(obj, MultiLink):
            # TODO
            pass
        else:
            raise NotImplementedError(f"MultiLink sub-object type {type(obj)} not implemented")
        join_table_type_names__predicates[(type_name, link.pred)].add(normalized_pred)

def build_multilink_tables_and_data(parent_type_name: str, target_type_name: str, preds: Set[str],
                                    scan_ids_types_resources: Mapping[int, Mapping[str, Iterable[Resource]]],
                                    connection) -> None:
    join_table_name = f"_{parent_type_name}_{target_type_name}"
    join_columns = [
        tableauhyperapi.TableDefinition.Column("scan_id", tableauhyperapi.SqlType.int(),
                                               nullability=tableauhyperapi.NOT_NULLABLE),
    ]
    # arn of parent object
    join_columns.append(
        tableauhyperapi.TableDefinition.Column(f"{parent_type_name}_arn", tableauhyperapi.SqlType.text(),
                                               nullability=tableauhyperapi.NOT_NULLABLE),
    )
    for pred in sorted(preds):
        join_columns.append(
            tableauhyperapi.TableDefinition.Column(pred, tableauhyperapi.SqlType.text(),
                                                   nullability=tableauhyperapi.NULLABLE)
        )
    join_table = tableauhyperapi.TableDefinition(
        table_name=join_table_name,
        columns=join_columns,
    )
    connection.catalog.create_table(join_table)
    # insert join data.
    with tableauhyperapi.Inserter(connection, join_table) as join_inserter:
        data = []
        for scan_id, types_resources in scan_ids_types_resources.items():
            for resource in types_resources[parent_type_name]:
                for link in resource.links:
                    if link.pred == target_type_name:
                        resource_data = [scan_id, resource.resource_id]
                        for pred in sorted(preds):
                            datapoint = None
                            for obj in link.obj:
                                if isinstance(obj, SimpleLink):
                                    if obj.pred == pred:
                                        datapoint = str(obj.obj)
                                elif type(obj) in (ResourceLinkLink, TransientResourceLinkLink):
                                    normalized_pred = f"{obj.pred}_arn"
                                    if normalized_pred == pred:
                                        datapoint = str(obj.obj)
                                elif isinstance(obj, MultiLink):
                                    pass # TODO
                                else:
                                    raise NotImplementedError(f"MultiLink sub-object type {type(obj)} not implemented")
                            resource_data.append(datapoint)
                        data.append(tuple(resource_data))
        join_inserter.add_rows(data)
        join_inserter.execute()

def get_pred_obj_type(pred: str, type_name: str, scan_ids_types_resources: Mapping[int, Mapping[str, Iterable[Resource]]]) -> Type:
    types = set()
    for types_resources in scan_ids_types_resources.values():
        for resource in types_resources[type_name]:
            for link in resource.links:
                if link.pred == pred:
                    types.add(type(link.obj))
        if types in ({str}, {list}, {int}, {bool}):
            return types.pop()
        raise Exception(f"I don't know how to handle {types}")


def pred_is_scalar(pred: str, resources: Iterable[Resource]) -> bool:
    for resource in resources:
        for link in resource.links:
            if link.pred == pred:
                if not isinstance(link.obj, str) and not isinstance(link.obj, int):
                    return False
    return True


def normalize_name(name: str) -> str:
    normalized_name = name
    if normalized_name.startswith("aws:"):
        normalized_name = normalized_name[len("aws:"):]
    normalized_name = normalized_name.replace(":", "_").replace("-", "_")
    return normalized_name

if __name__ == "__main__":
    sys.exit(main())
