#!/usr/bin/env python
# -*- coding: UTF-8 -*-




from typing import Optional
from pandas import DataFrame

from base import BaseObject
from cendalytics.tfidf.core.dmo import VectorSpaceTopNSelector


class ReadCollectionVectorSpace(BaseObject):
    """
    Purpose:
        Read the Vector Space and return the most discriminating terms (top-n)
            by key-field
    Sample Output:
        +----+--------+------------+
        |    |   Rank | Tag        |
        |----+--------+------------|
        |  0 |      1 | windows nt |
        |  1 |      2 | rfs        |
        |  2 |      3 | microsoft  |
        +----+--------+------------+
    """

    __df = None

    def __init__(self,
                 library_name: str,
                 is_debug: bool = False):
        """
        Created:
            10-Jul-2019
            craig.trim@ibm.com
            *   search the skills vector space
        Updated:
            5-Nov-2019
            craig.trim@ibm.com
            *   renamed from 'search-skills-vectorspace' and
                refactored out of the nlusvc project
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261
        :param library_name:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._library_name = library_name

    def _load_dataframe(self) -> None:
        from cendalytics.tfidf.core.dmo import VectorSpaceLibraryLoader
        self.__df = VectorSpaceLibraryLoader(is_debug=self._is_debug,
                                             library_name=self._library_name).df()

    def df(self) -> DataFrame:
        if self.__df is None:
            self._load_dataframe()
        return self.__df

    def process(self,
                key_field: str,
                expand: bool = False,
                top_n: int = 3) -> Optional[DataFrame]:
        """
        Purpose:
        :param expand:
        :param key_field:
            the key field to search the Vector Space on
            Key Fields by Collection:
                supply_tag_<date>       'Serial Number'
                demand_tag_<date>       'Open Seat ID'
                learning_tag_<date>     'LAID'
        :param top_n:
            the number of tags to search for
        :return:
            a pandas DataFrame of results
            Sample Output:
                +----+--------+------------+
                |    |   Rank | Tag        |
                |----+--------+------------|
                |  0 |      1 | windows nt |
                |  1 |      2 | rfs        |
                |  2 |      3 | microsoft  |
                +----+--------+------------+
        """
        selector = VectorSpaceTopNSelector(df=self.df(),
                                           is_debug=self._is_debug)
        return selector.process(top_n=top_n,
                                expand=expand,
                                key_field=key_field)
