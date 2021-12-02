#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from base import BaseObject
from base import FileIO


class GenerateSynonyms(BaseObject):

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
         Updated:
             21-Feb-2019
             craig.trim@ibm.com
             *   migrated to text
         Updated:
             11-Apr-2019
             craig.trim@ibm.com
             *   add synonym injection via process param list
                 and merging
         Updated:
             16-Jul-2019
             craig.trim@ibm.com
             *   remove '+' patterns from synonyms file
                 these belong exclusively to `patterns.py`
                 https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/440
         Updated:
             13-Dec-2019
             craig.trim@ibm.com
             *  add ontology-name as a param
                and load synonyms as a file
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
        Updated:
            14-Jan-2020
            craig.trim@ibm.com
            *   add further variation to injected synonyms
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1734#issuecomment-17146514
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_name = ontology_name

    def _synonyms(self,
                  injected_synonyms: dict) -> dict:
        from taskmda.mda.dmo import SynonymsKbReader
        from taskmda.mda.dmo import SynonymsDictGenerator

        # Step: Create Initial dictionary from CSV file
        data_frame = SynonymsKbReader.by_name(self._ontology_name).read_csv()
        synonyms = SynonymsDictGenerator(data_frame).process()

        # Step: Merge Injected Synonyms
        for k in injected_synonyms:
            if k not in synonyms:
                synonyms[k] = []
            for value in injected_synonyms[k]:
                synonyms[k].append(value)
                if '-' in value:  # GIT-1734-17146514
                    synonyms[k].append(value.replace('-', ' '))
                    synonyms[k].append(value.replace('-', ''))
                    synonyms[k].append(value.replace('-', '_'))

        # Step: Remove Duplicates
        _synonyms = {}
        for k in synonyms:
            _synonyms[k] = [x.strip() for x in sorted(set(synonyms[k]))
                            if x and "+" not in x]

        for k in synonyms:
            for v in synonyms[k]:
                if len(v) < 3:
                    print(f"Short Synonym Alert: (canon={k}, syn={v})")

        return _synonyms

    @classmethod
    def reverse(cls,
                synonyms: dict) -> dict:
        d_rev = {}

        for k in synonyms:
            for v in synonyms[k]:
                d_rev[v] = k

        return d_rev

    def process(self,
                injected_synonyms: dict):

        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess

        synonyms = self._synonyms(injected_synonyms)
        rev_syns = self.reverse(synonyms)

        def syns_to_file():
            the_json_result = pprint.pformat(synonyms, indent=4)
            the_json_result = "{0} = {{\n {1}".format(
                KbNames.synonyms(), the_json_result[1:])

            the_template_result = GenericTemplateAccess.process()
            the_template_result = the_template_result.replace(
                "CONTENT", the_json_result)

            path = os.path.join(os.environ["CODE_BASE"],
                                KbPaths.synonyms(self._ontology_name))
            FileIO.text_to_file(path, the_template_result)

        def rev_syns_to_file():
            the_json_result = pprint.pformat(rev_syns, indent=4)
            the_json_result = "{0} = {{\n {1}".format(
                KbNames.reverse_synonyms(), the_json_result[1:])

            the_template_result = GenericTemplateAccess.process()
            the_template_result = the_template_result.replace(
                "CONTENT", the_json_result)

            path = os.path.join(os.environ["CODE_BASE"],
                                KbPaths.reverse_synonyms(self._ontology_name))
            FileIO.text_to_file(path, the_template_result)

        syns_to_file()
        rev_syns_to_file()
