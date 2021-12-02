#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject
from base import FileIO


class DBpediaCategoryFilter(BaseObject):
    """ a DBpedia category may be generic to Wikipedia (hence the blacklist)

        but a category may also contain helpful categorical information:
        examples:

            entities                                    category
            IBM Information Management software         Information Management Software
    """

    _filtered_categories = None

    def __init__(self,
                 categories: list,
                 maximum_token_threshold: Optional[int] = None,
                 is_debug: bool = False):
        """
        Updated:
            23-Apr-2019
            craig.trim@ibm.com
            *   refactored out of script 'wikipedia-entity-analysis-02'
        Updated:
            10-Jul-2019
            craig.trim@ibm.com
            *   refactor to bring in line with the find-dbpedia-entry service
        Updated:
            8-Jan-2020
            craig.trim@ibm.com
            *   add maximum token threshold;
                any category composed of more tokens than this will be exclued
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710
        :param categories:
            any list of dbpedia categories
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._categories = categories
        self._blacklist = self._load_blacklist()
        self._maximum_token_threshold = maximum_token_threshold

    @staticmethod
    def _load_blacklist() -> list:
        path = "config/wikipedia/invalid_categories.txt"
        return FileIO.file_to_lines(FileIO.absolute_path(path),
                                    use_sort=False)

    def _is_valid(self,
                  a_category: str) -> bool:
        """
        Purpose:
            determine if the category is blacklisted

        Sample Input:
            [   'All articles with dead external links',
                'Articles with dead external links from July 2016',
                'Articles with permanently dead external links',
                'Divorce law' ]

        Sample Output:
            [   'Divorce law' ]

        Invalid categories are typically dbPedia specific
        and have little to do with the article itself

        :param a_category:
            any dbPedia category
        :return:
            True        if the category is valid
            False       if the category appears in a known blacklist
        """
        if "article" in a_category:
            return False
        if len(a_category.split()) > self._maximum_token_threshold:
            return False
        for item in self._blacklist:
            if item in a_category:
                return False
        return True

    def process(self) -> list:
        categories = [category.strip() for category in self._categories
                      if category]
        categories = [category for category in categories
                      if self._is_valid(category.lower())]

        return categories
