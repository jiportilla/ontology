#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from rdflib import Graph
from rdflib.namespace import Namespace

from base import BaseObject
from base import FileIO
from base import MandatoryParamError


class OwlGraphConnector(BaseObject):
    """Loads the Skills Ontology as a Graph into memory"""

    def __init__(self,
                 ontology_name: str,
                 some_owl_format: str = "ttl",
                 is_debug: bool = True):
        """
        Created:
            25-Feb-2019
            craig.trim@ibm.com
        Updated:
            14-Mar-2019
            craig.trim@ibm.com
            *   pass in params from consumer
        """
        BaseObject.__init__(self, __name__)
        if not ontology_name:
            raise MandatoryParamError("Ontology Name")

        _config = self._load_config()
        if ontology_name not in _config:
            raise ValueError("\n".join([
                "Ontology Name Unknown",
                f"Known Configuration: {_config.keys()}"]))

        self._owl_format = some_owl_format
        self._ns_path = _config[ontology_name]["ns"]
        self._owl_path = _config[ontology_name]["path"]
        self._ns_prefix = _config[ontology_name]["prefix"]

        if is_debug:
            self.logger.debug('\n'.join([
                f"Loading Ontology (name={ontology_name})",
                f"\tNS Path: {self._ns_path}",
                f"\tOWL Path: {self._owl_path}",
                f"\tPrefix: {self._ns_prefix}",
                f"\tFormat: {self._owl_format}"]))

    @staticmethod
    def _load_config() -> dict:
        """ configuration section """
        path = os.path.join(os.environ["CODE_BASE"],
                            "resources/config/config.yml")
        doc = FileIO.file_to_yaml(path)
        return doc["owl"]

    def _ontology_input_path(self) -> str:
        """ path to the ontology file """
        base = os.environ["CODE_BASE"]
        return os.path.join(base,
                            self._owl_path)

    def _load(self,
              input_path: str) -> Graph:
        """ load the ontology from disk as an RDF Graph """
        g = Graph()

        g.parse(input_path,
                format=self._owl_format)

        g.bind(self._ns_prefix,
               Namespace(self._ns_path))

        return g

    def process(self) -> Graph:
        """ load the ontology from disk as an RDF Graph """
        return self._load(self._ontology_input_path())
