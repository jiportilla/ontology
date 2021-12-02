#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import BaseObject
from base import FileIO
from base import MandatoryParamError


class ManifestConnectorForOwl(BaseObject):
    """ load an Ontology (OWL) file into an RDFLIB graph """

    def __init__(self,
                 some_ontology_manifest: dict):
        """
        Created:
            14-Mar-2019
            craig.trim@ibm.com
        :param some_ontology_manifest:
            the portion of the manifest that deals with loading an Ontology
                sample properties:
                  type: owl
                  input: /Users/craigtrim/Desktop/jrs.owl
                  output: /Users/craigtrim/Desktop/jrs-2.owl
                  prefix: jrs
                  namespace: http://www.semanticweb.org/craigtrim/ontologies/jrs#
        """
        BaseObject.__init__(self, __name__)
        if not some_ontology_manifest:
            raise MandatoryParamError("Ontology Manifest")

        self.ontology_manifest = some_ontology_manifest

    def _input_path(self) -> str:
        base = os.environ["CODE_BASE"]
        return os.path.join(base,
                            self.ontology_manifest["input"])

    def process(self) -> list:
        return FileIO.file_to_lines(self._input_path())
