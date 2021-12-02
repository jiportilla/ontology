#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import DataTypeError


class DBPediaContentNormalizer(BaseObject):
    """ Normalize the DBPedia Content Section """

    def __init__(self,
                 content: list,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            8-Jan-2020
            craig.trim@ibm.com
            *   add additional content normalization
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17014878
        """
        BaseObject.__init__(self, __name__)
        from nlusvc.core.bp import TextAPI

        if type(content) != list:
            raise DataTypeError("Content, list")

        self._content = content
        self._is_debug = is_debug
        self._text_api = TextAPI(is_debug=self._is_debug,
                                 ontology_name=ontology_name)

    def _normalize(self,
                   content: list) -> list:

        def _split() -> list:
            master = []

            for item in content:
                for line in item.split('\n'):
                    master.append(line)

            master = [line.strip() for line in master if line]
            master = [line for line in master if line and len(line)]

            return master

        def _sentencize(lines: list) -> list:

            master = []
            for line in lines:
                while '  ' in line:
                    line = line.replace('  ', ' ')

                [master.append(x) for x in self._text_api.sentencizer(input_text=line)]

            master = [line.strip() for line in master if line]
            master = [line for line in master if line and len(line)]

            return master

        return _sentencize(_split())

    def process(self) -> list:
        return self._normalize(self._content)
