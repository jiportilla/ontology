#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject


class DBpediaSeeAlsoExtractor(BaseObject):
    """ Extract one-or-more SeeAlso references

     Sample Input:
        ... text ...
        == See also ==
        Aldoxorubicin
        ... text ...

    Sample Output:
        ['Aldoxorubicin']
    """

    _filtered_categories = None

    def __init__(self,
                 content: list,
                 is_debug: bool = False):
        """
        Created:
            8-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17017220
        """
        BaseObject.__init__(self, __name__)
        self._content = content
        self.is_debug = is_debug

    def _extract(self) -> Optional[str]:
        extract = False
        for line in self._content:
            if extract:
                return line
            if '== See also ==' in line:
                extract = True

    def process(self) -> Optional[str]:
        content = self._extract()
        if not content:
            return None

        content = content.replace('"', '')

        return content
