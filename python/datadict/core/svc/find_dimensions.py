# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject
from base import RecordUnavailableRecord


class FindDimensions(BaseObject):
    """ Act as a Controller in front of all Dimesionality Dictionaries  """

    __unlisted = "unlisted"

    __blacklist = ['activity', 'agent', 'company', 'entity', 'industry', 'language', 'learning',
                   'provenance', 'role', 'situation', 'skill', 'state', 'root', 'telecommunication']

    @classmethod
    def sentiment(cls,
                  is_debug: bool = False) -> __name__:
        return FindDimensions(schema='sentiment',
                              is_debug=is_debug)

    def __init__(self,
                 schema: str,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            29-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15618121
        Updated:
            1-Nov-2019
            craig.trim@ibm.com
            *   add ability to search with or without underscores
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1151#issuecomment-15693382
        Updated:
            14-Nov-2019
            craig.trim@ibm.com
            *   update inner loop traversal
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1367#issuecomment-16010583
        Updated:
            21-Nov-2019
            craig.trim@ibm.com
            *   updates for see-also dict changes
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1195#issuecomment-16167608
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   add biotech dimenmsionality schema
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1609
        """
        BaseObject.__init__(self, __name__)
        from datadict import FindSynonym
        from datadict import FindRelationships

        self._is_debug = is_debug
        self._d_schema = self._load_schema(schema)

        self._syn_finder = FindSynonym(is_debug=is_debug,
                                       ontology_name=ontology_name)
        self._rel_finder = FindRelationships(is_debug=is_debug,
                                             ontology_name=ontology_name)

    @staticmethod
    def _load_schema(schema: str) -> dict:
        if schema.lower() == "learning":
            from datadict import the_dimesionality_learning_dict
            return the_dimesionality_learning_dict
        elif schema.lower() == "supply":
            from datadict import the_dimesionality_supply_dict
            return the_dimesionality_supply_dict
        elif schema.lower() == "degrees":
            from datadict import the_dimesionality_degrees_dict
            return the_dimesionality_degrees_dict
        elif schema.lower() == "majors":
            from datadict import the_dimesionality_majors_dict
            return the_dimesionality_majors_dict
        elif schema.lower() == "sentiment":
            from datadict import the_dimesionality_sentiment_dict
            return the_dimesionality_sentiment_dict
        elif schema.lower() == "biotech":
            from datadict import the_dimesionality_biotech_dict
            return the_dimesionality_biotech_dict
        raise NotImplementedError(schema)

    def children(self,
                 some_parent: str) -> list:
        some_parent = some_parent.lower().strip()
        if some_parent in self._d_schema:
            return self._d_schema[some_parent]
        raise RecordUnavailableRecord(some_parent)

    def top_level_entities(self) -> list:
        return sorted(self._d_schema.keys())

    def _find_in_schema(self,
                        input_text: str) -> set:
        matches = set()

        for k in self._d_schema:
            if k.lower() == input_text:
                matches.add(k)

            for v in self._d_schema[k]:
                if v.lower() == input_text:
                    matches.add(k)

        return matches

    @staticmethod
    def _cleanse_results(results: list) -> list:
        results = sorted(results)
        if not len(results):
            results = ["other"]

        if results == ['other', 'unlisted']:
            results = ['unlisted']

        if len(results) > 1 and 'unlisted' in results:
            results.remove('unlisted')

        if len(results) > 1 and 'other' in results:
            results = results.pop(results.index('other'))

        return results

    def find(self,
             input_text: str) -> list:
        cache = set()

        def _inner_find(some_input_text: str) -> Optional[list]:
            matches = set()
            if some_input_text in self.__blacklist:
                matches.add(self.__unlisted)

            schema_matches = self._find_in_schema(some_input_text)
            if len(schema_matches):
                matches = matches.union(schema_matches)

            else:

                if ' ' in some_input_text:
                    some_input_text = some_input_text.replace(' ', '_')
                elif '_' in some_input_text:
                    some_input_text = some_input_text.replace('_', ' ')

                schema_matches = self._find_in_schema(some_input_text)
                if len(schema_matches):
                    matches = matches.union(schema_matches)

                else:
                    for see_also in self._syn_finder.see_also(some_input_text):  # GIT-1195-16167608
                        if see_also not in cache:
                            cache.add(see_also)
                            matches = matches.union(_inner_find(see_also.lower().strip()))

                    some_input_text = some_input_text.replace('_', ' ')  # GIT-1367-16010583
                    parents = [x.lower().strip() for x in self._rel_finder.parents(some_input_text)
                               if x not in cache]
                    for parent in parents:
                        cache.add(parent)
                        matches = matches.union(_inner_find(parent))

            return matches

        results = _inner_find(input_text.lower().strip())
        results = self._cleanse_results(results)

        if self._is_debug:
            self.logger.debug(f"Located Schema ("
                              f"input={input_text}, "
                              f"schema={results})")
        return results
