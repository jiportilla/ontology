#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

import wikipedia
from wikipedia import WikipediaPage

from base import BaseObject


class DBpediaPageFinder(BaseObject):
    """ make a wire call to dbPedia to find a page """

    __d_disambiguation = {  # TODO: this should be list injected
        'cell': 'Cellular organizational structure',
        'substance': 'chemical substance',
        'condition': 'medical condition',
    }

    def __init__(self,
                 some_title: str,
                 is_debug: bool = False,
                 auto_suggest: bool = False):
        """
        Created:
            9-Jul-2019
            craig.trim@ibm.com
            *   refactored out of dbpedia-entity-lookup
        Updated:
            8-Jan-2020
            craig.trim@ibm.com
            *   update exception handling with logger statements
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710
        Updated:
            6-Feb-2020
            craig.trim@ibm.com
            *   update exception handling with key error
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1829#issuecomment-17622716
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._title = some_title
        self._auto_suggest = auto_suggest

        self.logger.debug('\n'.join([
            "Instantiated DBPediaPageFinder",
            f"\tTitle: {some_title}",
            f"\tAuto Suggest: {auto_suggest}"]))

    def process(self) -> Optional[WikipediaPage]:

        try:
            return wikipedia.page(title=self._title,
                                  pageid=None,
                                  preload=True,
                                  redirect=True,
                                  auto_suggest=self._auto_suggest)

        except wikipedia.exceptions.DisambiguationError as e:
            if self._title.lower() in self.__d_disambiguation:
                _title = self.__d_disambiguation[self._title.lower()]
                self.logger.debug('\n'.join([
                    "Forced Disambiguation",
                    f"\tOriginal Term: {self._title}",
                    f"\tDisambiguation: {_title}"]))
                return DBpediaPageFinder(some_title=_title,
                                         is_debug=self.is_debug).process()

            self.logger.error('\n'.join([
                "Disambiguation Error",
                f"\tTitle: {self._title}"]))
            self.logger.exception(e)

        except wikipedia.exceptions.PageError as e:
            self.logger.error('\n'.join([
                "Page Error",
                f"\tTitle: {self._title}"]))
            self.logger.exception(e)
            return None

        except KeyError as e:  # GIT-1829-17622716
            self.logger.error('\n'.join([
                "Key Error",
                f"\tTitle: {self._title}"]))
            self.logger.exception(e)
            return None

        except Exception as e:
            self.logger.error('\n'.join([
                "Generate Error",
                f"\tTitle: {self._title}"]))
            self.logger.exception(e)
            return None
