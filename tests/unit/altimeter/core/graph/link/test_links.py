import unittest

from rdflib import BNode, Graph, Literal, Namespace
from rdflib.term import URIRef

from altimeter.core.graph.exceptions import LinkParseException
from altimeter.core.graph.link.links import (
    MultiLink,
    ResourceLinkLink,
    SimpleLink,
    TagLink,
    TransientResourceLinkLink,
    link_from_dict,
)
from altimeter.core.graph.link.base import Link
from altimeter.core.graph.node_cache import NodeCache


class TestLinkFromDict(unittest.TestCase):
    def testMissingType(self):
        with self.assertRaises(LinkParseException):
            link_from_dict({})

    def testMissingPred(self):
        with self.assertRaises(LinkParseException):
            link_from_dict({"type": "simple"})

    def testMissingObj(self):
        with self.assertRaises(LinkParseException):
            link_from_dict({"type": "simple", "pred": "test-pred"})

    def testUnknownType(self):
        with self.assertRaises(LinkParseException):
            link_from_dict({"type": "fake-type", "pred": "test-pred", "obj": "test-obj"})

    def testSimpleType(self):
        link = link_from_dict({"type": "simple", "pred": "test-pred", "obj": "test-obj"})
        self.assertIsInstance(link, SimpleLink)

    def testTagType(self):
        link = link_from_dict({"type": "tag", "pred": "test-pred", "obj": "test-obj"})
        self.assertIsInstance(link, TagLink)

    def testResourceLinkType(self):
        link = link_from_dict({"type": "resource_link", "pred": "test-pred", "obj": "test-obj"})
        self.assertIsInstance(link, ResourceLinkLink)

    def testTransientResourceLinkType(self):
        link = link_from_dict(
            {"type": "transient_resource_link", "pred": "test-pred", "obj": "test-obj"}
        )
        self.assertIsInstance(link, TransientResourceLinkLink)

    def testMultiType(self):
        link = link_from_dict(
            {
                "type": "multi",
                "pred": "test-pred",
                "obj": [{"type": "simple", "pred": "test-int-pred", "obj": "test-int-obj"}],
            }
        )
        self.assertIsInstance(link, MultiLink)


class TestLinkSubclassing(unittest.TestCase):
    def testInitConcreteSubclass(self):
        with self.assertRaises(TypeError):

            class LinkSubClass(Link):
                def to_rdf(self, subj, namespace, graph, node_cache):
                    pass

    def testInitAbstractSubclass(self):
        class LinkSubClass(Link):
            pass


class TestSimpleLink(unittest.TestCase):
    def testToJson(self):
        pred = "test-pred"
        obj = "test-obj"
        link = SimpleLink(pred=pred, obj=obj)
        expected_link_dict = {"type": "simple", "pred": pred, "obj": obj}
        link_dict = link.to_dict()
        self.assertDictEqual(expected_link_dict, link_dict)

    def testToRdf(self):
        pred = "test-pred"
        obj = "test-obj"
        link = SimpleLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        results = graph.query("select ?s ?p ?o where {?s ?p ?o}")
        self.assertEqual(1, len(results))
        for result in results:
            s, p, o = result
            self.assertEqual(s, bnode)
            self.assertEqual(str(p), "test:test-pred")
            self.assertEqual(str(o), "test-obj")

    def testToRdfLargeInt(self):
        pred = "test-pred"
        obj = 2147483648
        link = SimpleLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        results = graph.query("select ?s ?p ?o where {?s ?p ?o}")
        self.assertEqual(1, len(results))
        for result in results:
            _, _, o = result
            self.assertEqual(
                o.datatype, URIRef("http://www.w3.org/2001/XMLSchema#nonNegativeInteger")
            )

    def testToRdfSmallInt(self):
        pred = "test-pred"
        obj = 20
        link = SimpleLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        results = graph.query("select ?s ?p ?o where {?s ?p ?o}")
        self.assertEqual(1, len(results))
        for result in results:
            _, _, o = result
            self.assertEqual(o.datatype, URIRef("http://www.w3.org/2001/XMLSchema#integer"))


class TestMultiLink(unittest.TestCase):
    def testToJson(self):
        pred = "test-multi-pred"
        obj = [
            SimpleLink(pred="test-simple-pred-1", obj="test-simple-obj-1"),
            SimpleLink(pred="test-simple-pred-1", obj="test-simple-obj-2"),
            SimpleLink(pred="test-simple-pred-2", obj="test-simple-obj-3"),
        ]
        link = MultiLink(pred=pred, obj=obj)
        expected_link_dict = {
            "pred": "test-multi-pred",
            "obj": [
                {"pred": "test-simple-pred-1", "obj": "test-simple-obj-1", "type": "simple"},
                {"pred": "test-simple-pred-1", "obj": "test-simple-obj-2", "type": "simple"},
                {"pred": "test-simple-pred-2", "obj": "test-simple-obj-3", "type": "simple"},
            ],
            "type": "multi",
        }
        link_dict = link.to_dict()
        self.assertDictEqual(expected_link_dict, link_dict)

    def testToRdf(self):
        pred = "test-multi-pred"
        obj = [
            SimpleLink(pred="test-simple-pred-1", obj="test-simple-obj-1"),
            SimpleLink(pred="test-simple-pred-1", obj="test-simple-obj-2"),
            SimpleLink(pred="test-simple-pred-2", obj="test-simple-obj-3"),
        ]
        link = MultiLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        results = graph.query(
            "select ?p ?o where {?s a <test:test-multi-pred> ; ?p ?o} order by ?p ?o"
        )
        result_tuples = []
        for result in results:
            self.assertEqual(2, len(result))
            result_tuples.append((str(result[0]), str(result[1])))
        expected_result_tuples = [
            ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "test:test-multi-pred"),
            ("test:test-simple-pred-1", "test-simple-obj-1"),
            ("test:test-simple-pred-1", "test-simple-obj-2"),
            ("test:test-simple-pred-2", "test-simple-obj-3"),
        ]
        self.assertEqual(result_tuples, expected_result_tuples)


class TestResourceLink(unittest.TestCase):
    def testToJson(self):
        pred = "test-pred"
        obj = "test-obj"
        link = ResourceLinkLink(pred=pred, obj=obj)
        self.assertDictEqual(link.to_dict(), {"type": "resource_link", "pred": pred, "obj": obj})

    def testToRdf(self):
        pred = "test-pred"
        obj = "test-obj"
        link = ResourceLinkLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        results = graph.query("select ?s ?p ?o where {?s ?p ?o}")
        obj_bnode = node_cache["test-obj"]
        self.assertEqual(1, len(results))
        for result in results:
            s, p, o = result
            self.assertEqual(s, bnode)
            self.assertEqual(str(p), "test:test-pred")
            self.assertEqual(o, obj_bnode)


class TestTransientResourceLink(unittest.TestCase):
    def testToJson(self):
        pred = "test-pred"
        obj = "test-obj"
        link = TransientResourceLinkLink(pred=pred, obj=obj)
        self.assertDictEqual(
            link.to_dict(), {"type": "transient_resource_link", "pred": pred, "obj": obj}
        )

    def testToRdf(self):
        pred = "test-pred"
        obj = "test-obj"
        link = TransientResourceLinkLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        results = graph.query("select ?s ?p ?o where {?s ?p ?o}")
        obj_bnode = node_cache["test-obj"]
        self.assertEqual(1, len(results))
        for result in results:
            s, p, o = result
            self.assertEqual(s, bnode)
            self.assertEqual(str(p), "test:test-pred")
            self.assertEqual(o, obj_bnode)


class TestTagLink(unittest.TestCase):
    def testToJson(self):
        pred = "test-pred"
        obj = "test-obj"
        link = TagLink(pred=pred, obj=obj)
        expected_link_dict = {"type": "tag", "pred": pred, "obj": obj}
        link_dict = link.to_dict()
        self.assertDictEqual(expected_link_dict, link_dict)

    def testToRdf(self):
        pred = "test-pred"
        obj = "test-obj"
        link = TagLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        tag_node = node_cache[f"{pred}:{obj}"]
        results = graph.query("select ?s ?p ?o where { ?s a <test:tag> ; ?p ?o } order by ?p ?o")
        expected_result_tuples = [
            (
                tag_node,
                URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                URIRef("test:tag"),
            ),
            (tag_node, URIRef("test:key"), Literal("test-pred")),
            (tag_node, URIRef("test:value"), Literal("test-obj")),
        ]
        result_tuples = []
        for result in results:
            s, p, o = result
            result_tuples.append((s, p, o))
        self.assertEqual(expected_result_tuples, result_tuples)

    def testToRdfWithCached(self):
        pred = "test-pred"
        obj = "test-obj"
        link = TagLink(pred=pred, obj=obj)
        bnode = BNode()
        graph = Graph()
        namespace = Namespace("test:")
        node_cache = NodeCache()
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        link.to_rdf(subj=bnode, namespace=namespace, graph=graph, node_cache=node_cache)
        tag_node = node_cache[f"{pred}:{obj}"]
        results = graph.query("select ?s ?p ?o where { ?s a <test:tag> ; ?p ?o } order by ?p ?o")
        expected_result_tuples = [
            (
                tag_node,
                URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                URIRef("test:tag"),
            ),
            (tag_node, URIRef("test:key"), Literal("test-pred")),
            (tag_node, URIRef("test:value"), Literal("test-obj")),
        ]
        result_tuples = []
        for result in results:
            s, p, o = result
            result_tuples.append((s, p, o))
        self.assertEqual(expected_result_tuples, result_tuples)
