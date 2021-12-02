#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO


class DBpediaReferencesFilter(BaseObject):
    """ a DBpedia reference may be generic to Wikipedia (hence the blacklist) """

    _filtered_categories = None

    def __init__(self,
                 references: list,
                 is_debug: bool = False):
        """
        Created:
            8-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17016775
        :param references:
            any list of dbpedia categories
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._references = references
        self._blacklist = self._load_blacklist()

    @staticmethod
    def _load_blacklist() -> list:
        path = "config/wikipedia/invalid_references.txt"
        return FileIO.file_to_lines(FileIO.absolute_path(path),
                                    use_sort=False)

    def _is_valid(self,
                  a_reference: str) -> bool:
        """
        Purpose:
            determine if the reference is blacklisted

        Sample Input:
            [   'https://web.archive.org/web/20170918185523/https://books.google.com/books?id=iDNy0XxGqT8C&pg=PA291',
                'https://voice.ons.org/news-and-views/outpatient-oncology-drug-series-doxorubicin-is-the-infamous-red-devil',
                'https://www.rcsb.org/structure/1D12',
                'https://www.wikidata.org/wiki/Q18936' ]

        Sample Output:
            [   'https://voice.ons.org/news-and-views/outpatient-oncology-drug-series-doxorubicin-is-the-infamous-red-devil',
                'https://www.rcsb.org/structure/1D12' ]

        Invalid references are typically dbPedia specific
        and have little to do with the article itself

        :param a_reference:
            any dbPedia reference
        :return:
            True        if the reference is valid
            False       if the reference appears in a known blacklist
        """
        for item in self._blacklist:
            if item.lower() in a_reference.lower():
                return False
        return True

    def process(self) -> list:
        references = [reference.strip() for reference in self._references
                      if reference]
        references = [reference for reference in references
                      if self._is_valid(reference.lower())]

        return references
