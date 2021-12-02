# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class FindDefines(BaseObject):
    """ act as a controller in front of all rel_defines_kb entries """

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            23-Apr-2019
            craig.trim@ibm.com
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   renamed from 'find-defining-urls'
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1580
        """
        BaseObject.__init__(self, __name__)
        from datadict.core.dmo import DictionaryLoader

        _loader = DictionaryLoader(is_debug=is_debug,
                                   ontology_name=ontology_name)

        self._all = _loader.relationships().defines()

    @staticmethod
    def _validate(some_url: str):
        some_url = some_url.lower().strip()
        if "http" not in some_url:
            return False
        if "wikipedia" not in some_url:
            return False
        return True

    def url(self,
            some_key: str) -> str:
        some_key = some_key.lower().strip()
        if some_key in self._all:

            for url in self._all[some_key]:
                if url and self._validate(url):
                    return url

            self.logger.warning(f"Invalid URL: {some_key}")
