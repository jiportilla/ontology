#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from rdflib import Graph
from rdflib import RDF
from rdflib import RDFS
from rdflib import URIRef

from base import BaseObject
from base import MandatoryParamError


class TransformOwltoJson(BaseObject):
    """ Transform an OWL Ontology Graph into an equivalent JSON Structure """

    def __init__(self,
                 some_name: str,
                 some_graph: Graph,
                 is_debug=False):
        """
        Created:
            14-Mar-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_name:
            raise MandatoryParamError("Ontology Name")
        if not some_graph:
            raise MandatoryParamError("OWL Graph")

        self._label_cache = {}

        self.name = some_name
        self.graph = some_graph
        self.is_debug = is_debug

    def _label(self,
               node: URIRef) -> str:
        """ return the first label only from a node """

        if node in self._label_cache:
            return self._label_cache[node]

        for _, _, o in self.graph.triples((node, RDFS.label, None)):
            self._label_cache[node] = o.title()
            return o.title()

    def _types_as_labels(self,
                         node: URIRef) -> list:
        """ return all the types as labels from a node """
        results = set()
        for _, _, o in self.graph.triples((node, RDF.type, None)):
            results.add(self._label(o))
        return sorted([x for x in results if x])

    def _comments(self,
                  node: URIRef) -> list:
        """ return all the comments as text from a node """
        results = set()
        for _, _, o in self.graph.triples((node, RDFS.comment, None)):
            results.add(o.title().lower().strip())
        return sorted([x for x in results if x])

    def _query_by_s_and_o(self,
                          s: URIRef,
                          p: URIRef) -> list:
        """ query an Ontology Graph by Subject and Object
            and return labels for answers """
        results = set()
        for _, _, o in self.graph.triples((s, p, None)):
            results.add(self._label(o))

        results = [x for x in results if x]
        return sorted(results)

    def _get_relationships(self) -> list:
        """ extract all custom predicates from the graph
        :return:
            unique set of custom predicates
        """
        rels = set()
        for _, p, _ in self.graph.triples((None, None, None)):
            if self.name in str(p):  # don't add known predicates like RDF.type
                rels.add(p)

        rels = sorted(rels)
        self.logger.debug("\n".join([
            "Extracted Predicates (total = {})".format(len(rels)),
            "\tvalues: {}".format(rels)
        ]))

        return rels

    @staticmethod
    def _relationship_as_label(p: URIRef) -> str:
        """ transform RDF predicate to label for
            sample input:
                rdflib.term.URIRef('http://www.semanticweb.org/craigtrim/ontologies/jrs#identifiedBy')
            sample output:
                identifiedBy    """
        rel = str(p)
        return rel.split("#")[-1].split("'")[0].strip()

    def _load_nodes(self) -> list:
        """ retrieve all the nodes from the ontology graph """
        all_nodes = self.graph.all_nodes()

        total_nodes = len(all_nodes)
        self.logger.debug("\n".join([
            "Loaded OWL Graph",
            "\ttotal-nodes: {}".format(total_nodes)
        ]))

        nodes = [x for x in all_nodes if type(x) == URIRef]
        return [x for x in nodes if x]

    def _generate_node(self,
                       node: URIRef,
                       predicates: list) -> dict:
        """ generate a single JSON dictionary from
            a single Ontology graph node """
        d_node = {}

        label = self._label(node)
        if not label:
            if self.is_debug:
                self.logger.warning("\n".join([
                    "No Label Exists",
                    "\tnode: {}".format(node)
                ]))
            return {}

        d_node["label"] = label
        d_node["types"] = self._types_as_labels(node)
        d_node["comments"] = self._comments(node)

        for predicate in predicates:
            _predicate_label = self._relationship_as_label(predicate)
            results = self._query_by_s_and_o(node, predicate)
            if len(results):
                d_node[_predicate_label] = results

        return d_node

    def process(self):
        nodes = self._load_nodes()
        relationships = self._get_relationships()

        l_graph = [self._generate_node(x, relationships) for x in nodes]
        l_graph = [x for x in l_graph if x and len(x)]

        self.logger.debug("\n".join([
            "Completed (total-keys = {})".format(len(l_graph))
        ]))

        return l_graph
