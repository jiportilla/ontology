#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO
from datagraph import OwlGraphConnector
from taskmda.mda.dmo import GenericTemplateAccess
from taskmda.mda.dmo import OntologyDictGenerator
from taskmda.mda.dto import KbNames
from taskmda.mda.dto import KbPaths


class GenerateRelationships(BaseObject):

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            28-Mar-2019
            craig.trim@ibm.com
            *   removed dead code
            *   updated logging statements
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   add ontology-name as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
            *   also renamed from 'generate-ontology-dict'
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   major refactoring in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1606
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._ontology_name = ontology_name
        self._graph = OwlGraphConnector(is_debug=self._is_debug,
                                        ontology_name=self._ontology_name).process()

    @classmethod
    def _write_dictionary(cls,
                          results: dict,
                          dictionary_name: str,
                          dictionary_path: str):
        the_json_result = pprint.pformat(results, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            dictionary_name, the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            dictionary_path)
        FileIO.text_to_file(path, the_template_result)

    def _ontology_dict_generator(self,
                                 query_name: str) -> dict:
        return OntologyDictGenerator(some_graph=self._graph,
                                     is_debug=self._is_debug,
                                     ontology_name=self._ontology_name,
                                     some_query_name=query_name).process()

    def _generate_implies_dictionary(self):
        results = self._ontology_dict_generator("find_implies_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_implies(),
                               KbPaths.rel_implies(self._ontology_name))

    def _generate_ownership_dictionary(self):
        results = self._ontology_dict_generator("find_owners_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_owns(),
                               KbPaths.rel_owns(self._ontology_name))

    def _generate_requires_dictionary(self):
        results = self._ontology_dict_generator("find_requires_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_requires(),
                               KbPaths.rel_requires(self._ontology_name))

    def _generate_produces_dictionary(self):
        results = self._ontology_dict_generator("find_produces_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_produces(),
                               KbPaths.rel_produces(self._ontology_name))

    def _generate_runson_dictionary(self):
        results = self._ontology_dict_generator("find_runson_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_runson(),
                               KbPaths.rel_runson(self._ontology_name))

    def _generate_version_dictionary(self):
        results = self._ontology_dict_generator("find_version_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_versions(),
                               KbPaths.rel_versions(self._ontology_name))

    def _generate_infinitive_dictionary(self):
        results = self._ontology_dict_generator("find_infinitive_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_infinitive(),
                               KbPaths.rel_infinitive(self._ontology_name))

    def _generate_similarity_dictionary(self):
        results = self._ontology_dict_generator("find_similarity_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_similarity(),
                               KbPaths.rel_similarity(self._ontology_name))

    def _generate_partof_dictionary(self):
        results = self._ontology_dict_generator("find_parts_query.txt")
        self._write_dictionary(results,
                               KbNames.rel_parts(),
                               KbPaths.rel_parts(self._ontology_name))

    def _generate_defined_by_dictionary(self):
        results = self._ontology_dict_generator("find_definedBy_query.txt")
        print(results)
        self._write_dictionary(results,
                               KbNames.rel_defines(),
                               KbPaths.rel_defines(self._ontology_name))

    def process(self):
        self._generate_defined_by_dictionary()
        self.logger.debug("Generated Defines Dictionary")

        self._generate_implies_dictionary()
        self.logger.debug("Generated Implications Dictionary")

        self._generate_infinitive_dictionary()
        self.logger.debug("Generated Infinitive Dictionary")

        self._generate_similarity_dictionary()
        self.logger.debug("Generated Similarity Dictionary")

        self._generate_ownership_dictionary()
        self.logger.debug("Generated Ownership Dictionary")

        self._generate_partof_dictionary()
        self.logger.debug("Generated Partonomy Dictionary")

        self._generate_requires_dictionary()
        self.logger.debug("Generated Requires Dictionary")

        self._generate_produces_dictionary()
        self.logger.debug("Generated Produces Dictionary")

        self._generate_runson_dictionary()
        self.logger.debug("Generated Runs-On Dictionary")

        self._generate_version_dictionary()
        self.logger.debug("Generated Versions Dictionary")
