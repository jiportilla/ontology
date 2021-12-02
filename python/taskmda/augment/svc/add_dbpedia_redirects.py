#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from pandas import DataFrame

from base import BaseObject
from nlusvc import TextAPI


class AddDBPediaRedirects(BaseObject):
    """ Service to find relevant dbPedia redirects """

    __df_redirects = None

    def __init__(self,
                 redirects_file: str,
                 ontology_name: str,
                 is_debug: bool = True):
        """
        Created:
            10-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1721
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

        self._text_api = TextAPI(is_debug=self._is_debug,
                                 ontology_name=ontology_name)

        if not self.__df_redirects:
            self.__df_redirects = self._load_redirects(redirects_file)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiated AddDBPediaRedirects",
                f"\tOntology Name: {ontology_name}",
                f"\tRedirects File: {redirects_file}",
                f"\tRedirects File Size: {len(self.__df_redirects)}"]))

    def _load_redirects(self,
                        redirects_file: str) -> DataFrame:
        from taskmda.augment.dmo import DBPediaRedirectReader
        return DBPediaRedirectReader(is_debug=self._is_debug,
                                     input_file=redirects_file).process()

    @staticmethod
    def _cleanse(results: list) -> list:
        s = set()

        for result in results:
            if ',' in result:
                result = result.replace(',', ' ')
            result = result.replace('_', ' ')
            while '  ' in result:
                result = result.replace('  ', ' ')

            s.add(result)

        return sorted(s)

    def _variations(self,
                    a_term: str) -> list:
        a_term = a_term.lower().strip()

        df2 = self.__df_redirects[self.__df_redirects['Term'] == a_term]
        if df2.empty:
            return []

        return sorted(df2['Variation'].unique())

    def _normalize(self,
                   terms: list) -> list:
        s = set()
        for term in terms:
            s.add(self._text_api.normalize(input_text=term))

        return sorted(s)

    @staticmethod
    def _augment(terms: list) -> list:
        s = set()

        for term in terms:
            s.add(term)

            if ' ' in term:
                tokens = term.split(' ')
                s.add('+'.join(tokens))
                s.add('_'.join(tokens))

        return sorted(s)

    @staticmethod
    def _print_csv(d_results: dict) -> None:
        for k in d_results:
            variants = ', '.join(sorted(d_results[k]))
            print(f"{k}~{variants}")

    def process(self,
                terms: list,
                print_csv: bool = True) -> dict:

        d_results = {}
        for term in terms:
            term = term.lower().replace(' ', '_')

            variants = self._variations(a_term=term)
            variants = self._cleanse(variants)
            variants = self._normalize(variants)
            variants = self._augment(variants)
            d_results[term] = variants

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Redirect Lookup Complete",
                f"\tInput: {terms}",
                pprint.pformat(d_results)]))

        if print_csv:
            self._print_csv(d_results)

        return d_results
