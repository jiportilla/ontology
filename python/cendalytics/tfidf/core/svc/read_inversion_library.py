#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject


class ReadInversionLibrary(BaseObject):
    """
    Purpose:
        Read an Inversion Library

    Sample Input:
        +------+------------+-------------------------------------------+
        |      | KeyField   | Tag                                       |
        |------+------------+-------------------------------------------|
        |    0 | 0697A5744  | cloud service                             |
        |    1 | 0697A5744  | data science                              |
        |    2 | 0697A5744  | solution design                           |
        |    3 | 05817Q744  | kubernetes                                |
        |    4 | 05817Q744  | bachelor of engineering                   |
        |    5 | 05817Q744  | developer                                 |
        ...
        | 1328 | 249045760  | electrical engineering                    |
        +------+------------+-------------------------------------------+

    Sample Output (given 'data science' as a term param)
        ['0697A5744']
    """

    def __init__(self,
                 library_name: str,
                 is_debug: bool = False):
        """
        Created:
            5-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15732844
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._library_name = library_name

    def _load_dataframe(self) -> DataFrame:
        from cendalytics.tfidf.core.dmo import InversionLibraryLoader
        return InversionLibraryLoader(is_debug=self._is_debug,
                                      library_name=self._library_name).df()

    def process(self,
                term: str) -> list:
        """
        Purpose:
            for a given term (skill) find the key fields (serial numbers) for which
                this term is the most discriminating
        :param term:
            any annotation tag within the Cendant Ontology
        :return:
            a list of key fields (e.g., Serial Numbers)
        """
        df = self._load_dataframe()

        df2 = df[df['Term'] == term.lower()]
        key_fields = sorted(df2['KeyField'].unique())

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Inversion Search Completed (total-results={len(key_fields)})",
                f"\tTerm: {term}",
                f"\tKey Fields: {key_fields}"]))

        return key_fields
