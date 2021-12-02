#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

import wikipedia
from redis import ResponseError

from base import BaseObject
from base import RedisClient


class DBpediaEntityResolution(BaseObject):
    """ Determine the most likely Wikipedia entry when multiple entries exist
        This function is biased toward preferring Software
    """

    _original_results = []
    _filtered_results = []
    _most_likely_result = None

    _redis = RedisClient(db=RedisClient.WIKI_SEARCH_DB)

    def __init__(self,
                 some_title: str,
                 is_debug: bool = False):
        """
        Updated:
            9-Jul-2019
            craig.trim@ibm.com
            *   refactored out of dbpedia-entity-lookup
        Updated:
            8-Jan-2020
            craig.trim@ibm.com
            *   comment out redis caching
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17015882
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._process(some_title)

    def _search(self,
                some_title: str) -> Optional[list]:
        """
        Search for a term in dbPedia
        Sample Input:
            'pandas'
        Sample Output:
            [ 'PANDAS',
              'Pandas (software)',
              'Giant panda',
              'Red panda',
              'Panda diplomacy',
              'List of giant pandas',
              'Giant pandas around the world',
              'Kung Fu Panda 3',
              'Fiat Panda' ]
        :param some_title:
            any title
        :return:
            all the possible results
        """

        if not self._redis.has(some_title):
            results = wikipedia.search(query=some_title,
                                       results=10,
                                       suggestion=True)
            results = [x for x in results[0] if x]

            self.logger.debug('\n'.join([
                "Wikipedia Search Complete",
                f"\tTitle: {some_title}",
                f"\tResults: {results}"]))

            self._original_results = results
            if results is None or not len(results):
                return None

            try:
                self._redis.set_list(some_title, results)
            except ResponseError as e:
                self.logger.error('\n'.join([
                    "Caching Failure",
                    f"\tEntity: {some_title}",
                    f"\Results (type={type(results)}): {results}"]))
                self.logger.exception(e)
                return [some_title]

        else:
            self.logger.debug(f"Retrieved Entity (name={some_title})")

        results = self._redis.get_list(some_title)
        if self._is_debug:
            self.logger.debug(f"Step 1: {results}")

        return results

    def _filter(self,
                some_title: str,
                results: list) -> list:
        """
        Filter a list of dbPedia results
        Sample Title:
            'pandas'
        Sample Results:
            [ 'PANDAS',
              'Pandas (software)',
              'Giant panda',
              'Red panda',
              'Panda diplomacy',
              'List of giant pandas',
              'Giant pandas around the world',
              'Kung Fu Panda 3',
              'Fiat Panda' ]
        Sample Output:
            [ 'PANDAS',
              'Pandas (software)',
              'List of giant pandas',
              'Giant pandas around the world' ]
        :param some_title:
            the original search string
        :param results:
            the original results from dbPedia
        :return:
            the filtered results
        """

        def inner() -> list:

            # match plural to singular form
            # e.g., 'drugs' to 'Drug'
            if some_title.lower().endswith('s'):  # has plural form
                singular = some_title[:len(some_title) - 1].lower()
                _results = [x for x in results if singular == x.lower().strip()]
                if not len(_results):
                    return [results[0]]
                return _results

            _results = [x for x in results if some_title.lower() in x.lower().strip()]
            if not len(_results):
                return [results[0]]
            return _results

        self._filtered_results = inner()
        if self._is_debug:
            self.logger.debug(f"Step 2: {self._filtered_results}")

        return self._filtered_results

    def _top(self,
             some_title: str,
             results: list) -> None:
        """
        Choose the most likely result from a list of results
        this function is biased toward choosing software based results

        Sample Input:
            [ 'PANDAS',
              'Pandas (software)',
              'List of giant pandas',
              'Giant pandas around the world' ]
        Sample Output:
            'Pandas (software)'

        :param some_title:
            the original search string
        :param results:
            the filtered results from a prior stage
        """

        filters = ["software", "programming"]

        def inner():
            def _filter(filter_term: str,
                        _results: list):
                _results = [x for x in _results if filter_term in x.lower()]
                if len(_results):
                    return _results[0]

            for a_filter in filters:
                result = _filter(a_filter, results)
                if result:
                    return result

            if len(results):
                return results[0]

        self._most_likely_result = inner()
        if self._is_debug:
            self.logger.debug('\n'.join([
                "dbPedia Entity Resolved",
                f"\tInput: {some_title}",
                f"\tOutput: {self._most_likely_result}"]))

    def _process(self,
                 some_title: str) -> None:
        """

        Sample Input:
            'pandas'

        Intermediate Output:
            Step 1:     ([  'PANDAS',
                            'Pandas (software)',
                            'Giant panda',
                            'Red panda',
                            'Panda diplomacy',
                            'List of giant pandas',
                            'Giant pandas around the world',
                            'Kung Fu Panda 3',
                            'Fiat Panda'], None)
            Step 2:     [   'PANDAS',
                            'Pandas (software)',
                            'Giant panda',
                            'Red panda',
                            'Panda diplomacy',
                            'List of giant pandas',
                            'Giant pandas around the world',
                            'Kung Fu Panda 3',
                            'Fiat Panda']
            Step 3:     [   'PANDAS',
                            'Pandas (software)',
                            'List of giant pandas',
                            'Giant pandas around the world']
            Step 4:     [   'Pandas (software)']

        Final Output:
            'Pandas (software)'

        :param some_title:
            any input title
        :return:
            the most likely wikipedia entry in the event of multiple entries
        """

        results = self._search(some_title)
        if results:
            results = self._filter(some_title, results)
            self._top(some_title, results)

    def original_results(self) -> list:
        """
        :return:
            all possible results from dbPedia
        """
        return self._original_results

    def filtered_results(self) -> list:
        """
        :return:
            a filtered list of probable results
        """
        return self._filtered_results

    def most_likely_result(self) -> Optional[str]:
        """
        :return:
            the single most likely result
        """
        return self._most_likely_result
