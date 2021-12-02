#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject
from cendalytics.wikipedia.ingest.dmo import DBpediaEntityResolution
from cendalytics.wikipedia.ingest.dmo import DBpediaRelationshipExtractor


class PartonomyFinder(BaseObject):
    """ Find a Partonomy Relationship in Text """

    def __init__(self,
                 d_page: dict,
                 is_debug: bool = False):
        """
        Created:
            6-Feb-2020
            craig.trim@ibm.com
            *   refactored out of 'create-cendant-entity'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1827
        """
        BaseObject.__init__(self, __name__)
        self._d_page = d_page
        self._is_debug = is_debug

    def _entity_finder(self,
                       some_name: str) -> Optional[str]:
        """
        Purpose:
            Find a dbPedia Entry for a given Term
        Notes:
            This is not an exact science as many terms are highly ambiguous
            in the case of multiple results we take the most likely match
        Sample Input:
            "beta"
        Sample Output:
            "Beta Tech"
        :param some_name:
            any input
        :return:
            a dbpedia entry
        """
        return DBpediaEntityResolution(is_debug=self._is_debug,
                                       some_title=some_name).most_likely_result()

    @staticmethod
    def _summary_input(d_page: dict) -> Optional[str]:
        if type(d_page['summary']) == list:
            if len(d_page['summary']):
                return d_page['summary'][0]
            return None

        return d_page['summary']['first']

    def process(self) -> list:

        def _is_not_title(a_result: str) -> bool:
            return a_result.lower().strip() != self._d_page['title'].lower().strip()

        results = DBpediaRelationshipExtractor(is_debug=self._is_debug,
                                               input_text=self._summary_input(self._d_page)).process()

        results = [self._entity_finder(result) for result in results]
        results = [result for result in results if result]
        results = [result for result in results
                   if _is_not_title(result)]

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Partonomy Results Generated",
                f"\tTitle: {self._d_page['title']}",
                f"\tInput {self._summary_input(self._d_page)}",
                f"\tResults: {results}"]))

        return results
