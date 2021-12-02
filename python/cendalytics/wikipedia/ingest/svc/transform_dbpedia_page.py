#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from wikipedia import WikipediaPage

from base import BaseObject
from cendalytics.wikipedia.ingest.dmo import DBPediaContentNormalizer
from nlusvc import TextAPI


class TransformDBPediaPage(BaseObject):
    """ for a given dbPedia page, create a JSON structure """

    _text_api = None

    def __init__(self,
                 some_title: str,
                 some_page: WikipediaPage,
                 is_debug: bool = False):
        """
        Created:
            9-Jul-2019
            craig.trim@ibm.com
            *   refactored out of dbpedia-entity-lookup
        Updated:
            8-Jan-2020
            craig.trim@ibm.com
            *   add additional content normalization
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17014878
            *   renamed from 'dbpedia-page-transform'
        """
        BaseObject.__init__(self, __name__)

        self._page = some_page
        self._title = some_title
        self.is_debug = is_debug
        self._text_api = TextAPI(is_debug=self.is_debug)

    def _normalized(self,
                    content: list or str) -> list:
        try:
            if type(content) == str:
                content = [content]
            return DBPediaContentNormalizer(is_debug=self.is_debug,
                                            ontology_name='biotech',
                                            content=content).process()
        except Exception as e:
            self.logger.error(e)

    def _summary(self,
                 some_page: WikipediaPage) -> list:
        return self._normalized(some_page.summary)

    def _content(self,
                 some_page: WikipediaPage) -> list:
        return self._normalized(some_page.content)

    def process(self) -> dict:

        def _references():
            try:
                return self._page.references
            except Exception as e:
                self.logger.error(e)

        def _categories():
            try:
                return self._page.categories
            except Exception as e:
                self.logger.error(e)

        def _sections():
            try:
                return self._page.sections
            except Exception as e:
                self.logger.error(e)

        def _parent_id():
            try:
                return str(self._page.parent_id)
            except Exception as e:
                self.logger.error(e)

        def _page_id():
            try:
                return str(self._page.pageid)
            except Exception as e:
                self.logger.error(e)

        def _revision_id():
            try:
                return str(self._page.revision_id)
            except Exception as e:
                self.logger.error(e)

        def _url():
            try:
                return self._page.url
            except Exception as e:
                self.logger.error(e)

        def _links():
            try:
                return self._page.links
            except Exception as e:
                self.logger.error(e)

        def _title():
            try:
                return self._page.title
            except Exception as e:
                self.logger.error(e)

        def _original_title():
            try:
                return self._page.original_title
            except Exception as e:
                self.logger.error(e)

        return {"key": self._title,
                "url": _url(),
                "title": _title(),
                "links": _links(),
                "content": self._content(self._page),
                "page_id": _page_id(),
                "summary": self._summary(self._page),
                "sections": _sections(),
                "parent_id": _parent_id(),
                "categories": _categories(),
                "references": _references(),
                "revision_id": _revision_id(),
                "original_title": _original_title()}
