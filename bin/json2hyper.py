#!/usr/bin/env python3
"""Convert Altimeter intermediate json to hyper format. This is experimental."""
import abc
import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from operator import attrgetter
from pathlib import Path
import sys
from typing import DefaultDict, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Type, Union
from types import MappingProxyType
import uuid

import tableauhyperapi

from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.link.base import Link
from altimeter.core.graph.link.links import (
    SimpleLink,
    ResourceLinkLink,
    TagLink,
    MultiLink,
    TransientResourceLinkLink,
)

Primitive = Union[int, bool, str, datetime, None]
TAG_TABLE_NAME = "tag"

# see https://github.com/python/mypy/issues/5374
@dataclass(frozen=True)  # type: ignore
class Column(abc.ABC):
    """Generic representation of a db column"""

    name: str

    @abc.abstractmethod
    def to_hyper(self) -> tableauhyperapi.TableDefinition.Column:
        """Create a hyper Column object from this Column"""
        raise NotImplementedError


@dataclass(frozen=True)
class PKColumn(Column):
    """Generic pk db column"""

    def to_hyper(self) -> tableauhyperapi.TableDefinition.Column:
        """Create a hyper Column object from this Column"""
        return tableauhyperapi.TableDefinition.Column(
            self.name, tableauhyperapi.SqlType.big_int(), nullability=tableauhyperapi.NOT_NULLABLE,
        )


@dataclass(frozen=True)
class FKColumn(Column):
    """Generic fk db column"""

    def to_hyper(self) -> tableauhyperapi.TableDefinition.Column:
        """Create a hyper Column object from this Column"""
        return tableauhyperapi.TableDefinition.Column(
            self.name, tableauhyperapi.SqlType.big_int(), nullability=tableauhyperapi.NULLABLE,
        )


@dataclass(frozen=True)
class TextColumn(Column):
    """Generic text db column"""

    def to_hyper(self) -> tableauhyperapi.TableDefinition.Column:
        """Create a hyper Column object from this Column"""
        return tableauhyperapi.TableDefinition.Column(
            self.name, tableauhyperapi.SqlType.text(), nullability=tableauhyperapi.NULLABLE,
        )


@dataclass(frozen=True)
class IntColumn(Column):
    """Generic integer db column"""

    def to_hyper(self) -> tableauhyperapi.TableDefinition.Column:
        """Create a hyper Column object from this Column"""
        return tableauhyperapi.TableDefinition.Column(
            self.name, tableauhyperapi.SqlType.big_int(), nullability=tableauhyperapi.NULLABLE,
        )


@dataclass(frozen=True)
class BoolColumn(Column):
    """Generic boolean db column"""

    def to_hyper(self) -> tableauhyperapi.TableDefinition.Column:
        """Create a hyper Column object from this Column"""
        return tableauhyperapi.TableDefinition.Column(
            self.name, tableauhyperapi.SqlType.bool(), nullability=tableauhyperapi.NULLABLE,
        )


@dataclass(frozen=True)
class TimestampColumn(Column):
    """Generic timestamp db column"""

    def to_hyper(self) -> tableauhyperapi.TableDefinition.Column:
        """Create a hyper Column object from this Column"""
        return tableauhyperapi.TableDefinition.Column(
            self.name, tableauhyperapi.SqlType.timestamp(), nullability=tableauhyperapi.NULLABLE,
        )


def normalize_name(name: str) -> str:
    normalized_name = name
    if normalized_name.startswith("aws:"):
        normalized_name = normalized_name[len("aws:") :]
    normalized_name = normalized_name.replace(":", "_").replace("-", "_")
    return normalized_name


def build_table_defns(graph_sets: Iterable[GraphSet]) -> Mapping[str, Tuple[Column, ...]]:
    # discover simple link obj types - generally str, bool or int
    table_names_simple_obj_types: DefaultDict[
        str, DefaultDict[str, Set[Type[Primitive]]]
    ] = defaultdict(lambda: defaultdict(set))
    for graph_set in graph_sets:
        for resource in graph_set.resources:
            table_name = normalize_name(resource.type_name)
            for link in resource.links:
                if isinstance(link, SimpleLink):
                    table_names_simple_obj_types[table_name][link.pred].add(type(link.obj))
    table_names_columns: DefaultDict[str, Set[Column]] = defaultdict(set)
    for graph_set in graph_sets:
        for resource in graph_set.resources:
            table_name = normalize_name(resource.type_name)
            # all top level types have an id column
            table_names_columns[table_name].add(PKColumn(f"_{table_name}_id"))
            # all top level types have an arn
            table_names_columns[table_name].add(TextColumn(f"{table_name}_arn"))
            links = resource.links
            simple_obj_types = MappingProxyType(table_names_simple_obj_types[table_name])
            build_table_defns_helper(
                links, table_name, table_names_columns, simple_obj_types,
            )
    table_defns = {
        table_name: tuple(sorted(columns, key=attrgetter("name")))
        for table_name, columns in table_names_columns.items()
    }
    # define the tag table
    tag_columns = (
        FKColumn("resource_id"),
        TextColumn("key"),
        TextColumn("value"),
    )
    table_defns[TAG_TABLE_NAME] = tag_columns
    return MappingProxyType(table_defns)


def build_table_defns_helper(
    links: Iterable[Link],
    table_name: str,
    table_names_columns: Dict[str, Set[Column]],
    simple_obj_types: Mapping[str, Set[Type[Primitive]]],
) -> None:
    for link in links:
        if isinstance(link, SimpleLink):
            simple_column: Union[IntColumn, BoolColumn, TextColumn]
            if simple_obj_types[link.pred] == set((int,)):
                simple_column = IntColumn(name=link.pred,)
            elif simple_obj_types[link.pred] == set((bool,)):
                simple_column = BoolColumn(name=link.pred,)
            else:
                simple_column = TextColumn(name=link.pred,)
            table_names_columns[table_name].add(simple_column)
        elif type(link) in (ResourceLinkLink, TransientResourceLinkLink):
            table_names_columns[table_name].add(FKColumn(name=f"_{link.pred}_id",))
        elif isinstance(link, MultiLink):
            multilink_table_name = f"_{table_name}_{link.pred}"
            # multilink tables have a pk id column and a fk to the parent
            table_names_columns[multilink_table_name].add(PKColumn(f"_{multilink_table_name}_id",))
            table_names_columns[multilink_table_name].add(FKColumn(name=f"_{table_name}_id",))
            build_table_defns_helper(
                links=link.obj,
                table_name=multilink_table_name,
                table_names_columns=table_names_columns,
                simple_obj_types=simple_obj_types,
            )
        elif isinstance(link, TagLink):
            pass  # a single Tags table is built in build_table_defns
        else:
            raise NotImplementedError(f"Link type {type(link)} is not implemented")


def build_data(
    graph_sets: Iterable[GraphSet], table_defns: Mapping[str, Tuple[Column, ...]],
) -> Mapping[str, List[Tuple[Primitive, ...]]]:
    pk_counters: DefaultDict[str, int] = defaultdict(int)
    arns_pks: Dict[str, int] = {}
    table_names_datas: DefaultDict[str, List[Tuple[Primitive, ...]]] = defaultdict(list)
    for graph_set in graph_sets:
        for resource in graph_set.resources:
            resource_data: List[Primitive] = []
            table_name = normalize_name(resource.type_name)
            arn = resource.resource_id
            # build a primary key for this resource
            pk = arns_pks.get(arn)
            if pk is None:
                pk_counters[table_name] += 1
                pk = pk_counters[table_name]
                arns_pks[arn] = pk
            # add tag data
            for link in resource.links:
                if isinstance(link, TagLink):
                    tag_key, tag_value = link.pred, link.obj
                    table_names_datas[TAG_TABLE_NAME].append((pk, tag_key, tag_value))
            # iterate over the columns we expect the corresponding resource for this
            # table and fill them by looking for corresponding values in the resource
            for column in table_defns[table_name]:
                if isinstance(column, PKColumn):
                    if not pk:
                        raise Exception(
                            f"BUG: No pk found for {table_name} : {table_defns[table_name]}"
                        )
                    resource_data.append(pk)
                elif isinstance(column, FKColumn):
                    column_link_name = column.name.lstrip("_")[:-3]  # strip leading _, and _id
                    fk = None
                    for candidate_fk_link in resource.links:
                        if type(candidate_fk_link) in (ResourceLinkLink, TransientResourceLinkLink):
                            if candidate_fk_link.pred == column_link_name:
                                f_table_name = normalize_name(candidate_fk_link.pred)
                                f_arn = candidate_fk_link.obj
                                fk = arns_pks.get(f_arn)
                                if fk is None:
                                    pk_counters[f_table_name] += 1
                                    fk = pk_counters[f_table_name]
                                    arns_pks[f_arn] = fk
                                break
                    resource_data.append(fk)
                elif isinstance(column, TextColumn):
                    text_data = None
                    if column.name == "arn":
                        text_data = resource.resource_id
                    else:
                        for candidate_simple_link in resource.links:
                            if isinstance(candidate_simple_link, SimpleLink):
                                if candidate_simple_link.pred == column.name:
                                    text_data = str(candidate_simple_link.obj)
                                    break
                    resource_data.append(text_data)
                elif type(column) in (IntColumn, BoolColumn, TimestampColumn):
                    bool_or_int_or_timestamp_data = None
                    for candidate_simple_link in resource.links:
                        if isinstance(candidate_simple_link, SimpleLink):
                            if candidate_simple_link.pred == column.name:
                                bool_or_int_or_timestamp_data = candidate_simple_link.obj
                                break
                    resource_data.append(bool_or_int_or_timestamp_data)
                else:
                    raise NotImplementedError(f"Column type {type(column)} not implemented")
            table_names_datas[table_name].append(tuple(resource_data))
            # now look for MultiLinks in this resource.  Each of these represent a table
            for link in resource.links:
                if isinstance(link, MultiLink):
                    build_multilink_data(
                        pk_counters=pk_counters,
                        arns_pks=arns_pks,
                        table_names_datas=table_names_datas,
                        table_defns=table_defns,
                        multi_link=link,
                        parent_table_name=table_name,
                        parent_pk=pk,
                    )
    return MappingProxyType(table_names_datas)


def build_multilink_data(
    pk_counters: Dict[str, int],
    arns_pks: Dict[str, int],
    table_names_datas: DefaultDict[str, List[Tuple[Primitive, ...]]],
    table_defns: Mapping[str, Tuple[Column, ...]],
    parent_table_name: str,
    parent_pk: int,
    multi_link: MultiLink,
) -> None:
    table_name = f"_{parent_table_name}_{multi_link.pred}"
    resource_data: List[Primitive] = []
    columns = table_defns[table_name]
    # assign a pk. MultiLinks don't really have arns so we create a uuid
    arn = str(uuid.uuid4())
    pk = arns_pks.get(arn)
    if pk is None:
        pk_counters[table_name] += 1
        pk = pk_counters[table_name]
        arns_pks[arn] = pk
    # MultiLinks always have a primary key and then a parent fk
    resource_data.append(pk)
    resource_data.append(parent_pk)
    for column in columns[2:]:  # skip the primary key and parent fk
        if isinstance(column, FKColumn):
            fk = None
            for candidate_resource_link in multi_link.obj:
                if type(candidate_resource_link) in (ResourceLinkLink, TransientResourceLinkLink):
                    pred = f"_{candidate_resource_link.pred}_id"
                    if pred == column.name:
                        f_arn = candidate_resource_link.obj
                        f_arn_parts = f_arn.split(":")
                        f_arn_svc = f_arn_parts[2]
                        f_arn_type = f_arn_parts[5].split("/")[0]
                        fk = arns_pks.get(f_arn)
                        if fk is None:
                            f_table_name = "_".join((f_arn_svc, f_arn_type))
                            pk_counters[f_table_name] += 1
                            fk = pk_counters[f_table_name]
                            arns_pks[f_arn] = fk
                        break
            resource_data.append(fk)
        elif isinstance(column, TextColumn):
            text_data = None
            for candidate_simple_link in multi_link.obj:
                if isinstance(candidate_simple_link, SimpleLink):
                    if candidate_simple_link.pred == column.name:
                        text_data = str(candidate_simple_link.obj)
                        break
            resource_data.append(text_data)
        elif type(column) in (IntColumn, BoolColumn):
            bool_or_int_data = None
            for candidate_simple_link in multi_link.obj:
                if isinstance(candidate_simple_link, SimpleLink):
                    if candidate_simple_link.pred == column.name:
                        bool_or_int_data = candidate_simple_link.obj
                        break
            resource_data.append(bool_or_int_data)
        else:
            raise NotImplementedError(f"Column type {type(column)} not implemented")

    # now recurse
    for link in multi_link.obj:
        if isinstance(link, MultiLink):
            build_multilink_data(
                pk_counters=pk_counters,
                arns_pks=arns_pks,
                table_names_datas=table_names_datas,
                table_defns=table_defns,
                parent_table_name=table_name,
                parent_pk=pk,
                multi_link=link,
            )

    table_names_datas[table_name].append(tuple(resource_data))


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("input_json_filepaths", type=Path, nargs="+")
    args_ns = parser.parse_args(argv)

    input_json_filepaths = args_ns.input_json_filepaths
    if len(input_json_filepaths) > 1:
        raise NotImplementedError("Only one input supported at this time")

    # create a dict of scan ids to GraphSets. This contains all of the data in the provided input.
    scan_ids_graph_sets: Dict[int, GraphSet] = {
        scan_id: GraphSet.from_json_file(filepath)
        for scan_id, filepath in enumerate(input_json_filepaths)
    }

    # discover tables which need to be created by iterating over resources and finding the maximum
    # set of predicates used for each type
    table_defns = build_table_defns(scan_ids_graph_sets.values())

    # build data
    table_names_datas = build_data(scan_ids_graph_sets.values(), table_defns)

    table_names_tables: Dict[str, tableauhyperapi.TableDefinition] = {}
    with tableauhyperapi.HyperProcess(
        telemetry=tableauhyperapi.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
    ) as hyper:
        with tableauhyperapi.Connection(
            endpoint=hyper.endpoint,
            database="altimeter.hyper",
            create_mode=tableauhyperapi.CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            # create tables
            for table_name, columns in table_defns.items():
                table = tableauhyperapi.TableDefinition(
                    table_name=table_name, columns=[column.to_hyper() for column in columns]
                )
                connection.catalog.create_table(table)
                table_names_tables[table_name] = table

            for table_name, datas in table_names_datas.items():
                with tableauhyperapi.Inserter(
                    connection, table_names_tables[table_name]
                ) as inserter:
                    inserter.add_rows(datas)
                    inserter.execute()

    return 0


if __name__ == "__main__":
    sys.exit(main())
