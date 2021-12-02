#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from datamongo import CendantSrc
from datamongo.core.dmo import BaseMongoClient


class SearchSourceCollection(BaseObject):
    """ Perform a Text Query on a Cendant Source Collection

    Sample Output:
        +-----+---------------------------------------+--------+---------------------------------+
        |     | A                                     | B      | C                               |
        |-----+---------------------------------------+--------+---------------------------------|
        |   0 | an MVP for                            | coffee | recommendation based            |
        |   1 | a chain of                            | coffee | retailers within                |
        |   2 | while installing new                  | coffee | machines, tea                   |
        |   3 | plan events -                         | coffee | with leaders                    |
        |   4 | taekwondo, karate, handicrafts        | coffee | making. She                     |
        |   5 | Town Hall session,                    | coffee | session, 1-to-1                 |
        ...
        | 558 | for forecast of                       | coffee | and tea                         |
        +-----+---------------------------------------+--------+---------------------------------+

    """

    _records = None

    def __init__(self,
                 name: str,
                 term: str,
                 window_size: int,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = False):
        """
        Created:
            16-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1122
        :param mongo_client:
            the instantiated connection to mongoDB
        :param is_debug:
            True        write debug statements to the console
        """
        BaseObject.__init__(self, __name__)

        self._name = name
        self._term = term
        self._is_debug = is_debug
        self._window_size = window_size
        self._mongo_client = mongo_client

    def _cendant_src(self):
        return CendantSrc(collection_name=self._name,
                          mongo_client=self._mongo_client,
                          is_debug=self._is_debug)

    def _full_text_search(self,
                          cendant_src: CendantSrc) -> dict:

        d_results = cendant_src.full_text_search(some_term=self._term)
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Full Text Search Completed",
                f"\tCollection Name: {self._name}",
                f"\tSearch Term: {self._term}",
                f"\tResults: {len(d_results)}"]))

        return d_results

    def _window_query(self,
                      d_results: dict):
        from datamongo.text.dmo import TextQueryWindower

        windower = TextQueryWindower(query_results=d_results,
                                     is_debug=self._is_debug)

        df_results = windower.process(term=self._term,
                                      window_size=self._window_size)

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Query Results (term={self._term}, total={len(df_results)})",
                tabulate(df_results,
                         headers='keys',
                         tablefmt='psql')]))

        return df_results

    def process(self) -> DataFrame:
        """
        :return:
            a DataFrame of results
        """
        cendant_src = self._cendant_src()
        d_results = self._full_text_search(cendant_src)
        df_results = self._window_query(d_results)

        return df_results
