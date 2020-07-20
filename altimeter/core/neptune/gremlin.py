#!/usr/bin/env python3
"""Scratchpad file
"""
import csv
import sys
import json
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import *
from gremlin_python.structure.graph import Path, Vertex, Edge
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from altimeter.core.neptune.client import NeptuneEndpoint


logger = Logger()


class AttrDict(dict):
    def __getattr__(self, item):
        return self[item]


def check_datatype_compatability(data):
    # Need to handle numbers that are bigger than a Long in Java, for now we stringify it
    if isinstance(data, int) and (data > 9223372036854775807 or data < -9223372036854775807):
        return str(data)
    else:
        return data


def process_resource(resource, r, vertices, edges):
    row = AttrDict()
    row['~id'] = r
    row['~label'] = resource['type']
    for l in resource['links']:
        if l['type'] == 'simple':
            row[l['pred']] = l['obj']
        elif l['type'] == 'multi':  # TODO - Revisit this logic and clean it up for nested multi objects
            objs = l['obj']
            for o in objs:
                flatten_multi(l['pred'], o, row)
        elif l['type'] == 'resource_link' or l['type'] == 'transient_resource_link':
            edge = AttrDict()
            edge['~id'] = f'{l["pred"]}_{r}_{l["obj"]}'
            edge['~label'] = l['type']
            edge['~from'] = r
            edge['~to'] = l['obj']
            edges.append(edge)
        elif l['type'] == 'tag':
            # Don't add if there is a duplicate
            if not any(x['~id'] == l['pred'] + ':' + l['obj'] for x in vertices):
                tagRow = AttrDict()
                tagRow['~id'] = l['pred'] + ':' + l['obj']
                tagRow['~label'] = l['type']
                tagRow[l['pred']] = l['obj']
                vertices.append(tagRow)
            edge = AttrDict()
            edge['~id'] = f'{l["pred"]}_{r}_{l["obj"]}'
            edge['~label'] = 'tagged'
            edge['~from'] = r
            edge['~to'] = l['pred'] + ':' + l['obj']
            edges.append(edge)
        else:
            print(f'Unknown {l["type"]}')
    vertices.append(row)


def flatten_multi(pred, o, row):
    if o['type'] == 'simple':
        row[pred + '.' + o['pred']] = o['obj']
    elif o['type'] == 'multi':
        objs = o['obj']
        for o2 in objs:
            flatten_multi(pred, o2, row)


def write_vertices(g, vertices):
    for r in vertices:
        t = g.V(r['~id']).fold().coalesce(
            __.unfold(), __.addV(r['~label']).property(T.id, r['~id']))
        for k in r.keys():
            if not k in ['~id', '~label']:
                t = t.property(k, check_datatype_compatability(r[k]))
        t.next()


def write_edges(g, edges):
    for r in edges:
        (g.V(r['~from']).fold().
            coalesce(
                __.unfold(),
                __.addV().property(T.id, r['~from'])
        ).store('from').
            V(r['~to']).fold().
            coalesce(
            __.unfold(),
            __.addV('unspecified').property(T.id, r['~to'])
        ).store('to').
            inE().hasId(r['~id']).
            fold().
            coalesce(
                __.unfold(),
                __.addE(r['~label']).property(T.id, r['~id']).
            from_(__.select('from').unfold()).to(__.select('to').unfold())
        )
        ).next()


def main() -> int:
    server = 'altimeter-226036490.us-west-2.elb.amazonaws.com'
    port = 80
    vertices = []
    edges = []
    with open('data.json', 'r') as f:
        json_string = f.read()
        resources = json.loads(json_string)
    for r in resources["resources"]:
        process_resource(resources["resources"][r], r, vertices, edges)

    write_to_neptune(server, port, vertices, edges)


def write_to_neptune(server, port, vertices, edges):
    logger.debug(f'Opening connection to {server}:{port}')
    endpoint = NeptuneEndpoint(host=server, port=port, ssl=false)
    g, conn = connectToNeptune(server, port, False)
    logger.info('Begin Writing Vertices')
    write_vertices(g, vertices)
    logger.info('Begin Writing Edges')
    write_edges(g, edges)
    logger.debug(f'Closing connection to {server}:{port}')
    conn.close()


def write_to_csv(vertices, edges):
    logger.info('Begin File Export')
    csv_columns = list(set(val for dic in vertices for val in dic.keys()))
    csv_columns.sort()
    csv_file = 'vertices.csv'
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in vertices:
                writer.writerow(data)
    except IOError as err:
        print(f'I/O error: {err}')

    csv_columns = list(set(val for dic in edges for val in dic.keys()))
    csv_columns.sort()
    csv_file = 'edges.csv'
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in edges:
                writer.writerow(data)
    except IOError as err:
        print(f'I/O error: {err}')


if __name__ == "__main__":
    sys.exit(main())
