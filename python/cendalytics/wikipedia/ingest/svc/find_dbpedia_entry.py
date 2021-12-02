#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from wikipedia import WikipediaPage

from base import BaseObject


class FindDbPediaEntry(BaseObject):
    """ for a given input find the associated dbPedia entry """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            9-Jul-2019
            craig.trim@ibm.com
        Updated:
            8-Jan-2020
            craig.trim@ibm.com
            *   major refactoring
            *   redis doesn't belong in this stage; remove
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17015882
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def process(self,
                some_input: str) -> Optional[dict]:
        from cendalytics.wikipedia.ingest.dmo import DBpediaPageFinder
        from cendalytics.wikipedia.ingest.svc import TransformDBPediaPage
        from cendalytics.wikipedia.ingest.dmo import DBpediaEntityResolution

        def _entity_resolver(an_input: str) -> DBpediaEntityResolution:
            """ resolve ambiguous title inputs """
            return DBpediaEntityResolution(some_title=an_input,
                                           is_debug=self._is_debug)

        def _page(some_title: str) -> Optional[WikipediaPage]:
            try:
                return DBpediaPageFinder(some_title=some_title,
                                         is_debug=self._is_debug).process()
            except Exception:
                return None

        the_title = _entity_resolver(some_input).most_likely_result()
        the_page = _page(the_title)
        if not the_page:
            return None

        the_entry = TransformDBPediaPage(some_page=the_page,
                                         some_title=the_title,
                                         is_debug=self._is_debug).process()

        is_valid_entry = the_entry is not None or the_entry != {} or len(the_entry) > 0
        if not is_valid_entry:
            return None

        return the_entry
