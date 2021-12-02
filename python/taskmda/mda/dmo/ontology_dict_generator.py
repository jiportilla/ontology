#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from rdflib import Graph
from rdflib.plugins.sparql.processor import SPARQLResult
from rdflib.query import ResultRow

from base import BaseObject
from base import FileIO
from base import MandatoryParamError
from datadict import FindEntity


class OntologyDictGenerator(BaseObject):

    def __init__(self,
                 some_graph: Graph,
                 some_query_name: str,
                 ontology_name: str,
                 lowercase_keys: bool = False,
                 is_debug: bool = False):
        """
        Created:
            27-Feb-2019
            craig.trim@ibm.com
            *   refactored out of generate-ontology-dict
        Updated:
            5-Mar-2019
            craig.trim@ibm.com
            *   renamed from 'ontology-relationship-generator'
        Updated:
            29-Apr-2019
            craig.trim@ibm.com
            *   added value-errors if labels are none
        Updated:
            8-May-2019
            craig.trim@ibm.com
            *   added comprehension to '_normalize' function
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   add ontology-name parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1606
        """
        BaseObject.__init__(self, __name__)
        if not some_graph:
            raise MandatoryParamError("Ontology Graph")

        self._graph = some_graph
        self._is_debug = is_debug
        self._query_name = some_query_name
        self._lowercase_keys = lowercase_keys

        self.entity_finder = FindEntity(is_debug=is_debug,
                                        ontology_name=ontology_name)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiated Ontology Dictionary Generator",
                f"\tQuery Name: {self._query_name}",
                f"\tOntology Name: {ontology_name}",
                f"\tLowerCase Keys? {self._lowercase_keys}"]))

    @staticmethod
    def _path(file_name: str) -> str:
        path = os.path.join(os.environ["CODE_BASE"],
                            "resources/ontology/queries",
                            file_name)
        return FileIO.file_to_string(path)

    def _subject_label(self,
                       row: ResultRow):
        s_label = self.entity_finder.label_or_self(row[0].title())
        if self._lowercase_keys:
            s_label = s_label.lower()

        if not s_label or not len(s_label):
            self.logger.error(f"Unexpected Subject Label "
                              f"(row={row})")
            raise ValueError

        return s_label

    def _object_label(self,
                      row: ResultRow,
                      object_label_lookup: bool):

        def _o_label():
            if "http" in row[1]:
                return str(row[1]).split("'")[-1].split("'")[0].strip()
            return row[1].title()

        o_label = _o_label()
        if not o_label or not len(o_label):
            raise ValueError("\n".join([
                "Object Label is None (row={})".format(
                    row)]))

        if object_label_lookup and "http" not in o_label:
            return self.entity_finder.label(o_label)

        return o_label

    def _ontology_query(self,
                        object_label_lookup: bool) -> dict:
        d = {}

        def _update_dict(query_results: SPARQLResult):
            for row in query_results:

                s_label = self._subject_label(row)

                if s_label not in d:
                    d[s_label] = set()
                d[s_label].add(self._object_label(row,
                                                  object_label_lookup))

        def _query(query_name: str) -> SPARQLResult:
            return self._graph.query(
                self._path(query_name))

        def _normalize() -> dict:
            _results = {}
            for k in d:
                values = [x for x in d[k] if x]
                _results[k] = sorted(values)
            return _results

        _update_dict(_query(self._query_name))
        return _normalize()

    def process(self,
                object_label_lookup: bool = True) -> dict:
        return self._ontology_query(object_label_lookup)
