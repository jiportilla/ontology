#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import LoadStopWords
from nlusvc import TextAPI


class DBpediaTermExtractor(BaseObject):
    """
    Purpose:
    Extract Cased Terms

    sample input:
        One of these services is Amazon Elastic Compute Cloud, which allows users to
        have at their disposal a virtual cluster of computers, available all the time,
        through the Internet

    sample output:
        Amazon Elastic Compute Cloud """

    def __init__(self,
                 entry: dict,
                 is_debug: bool = False):
        """
        Updated:
            23-Apr-2019
            craig.trim@ibm.com
            *   refactored out of script 'wikipedia-entity-analysis-05'
        :param d_entities:
            the dictionary generated from 'dbpedia-entity-lookup'
        """
        BaseObject.__init__(self, __name__)
        self.entry = entry
        self.stopwords = LoadStopWords().load()
        self._api = TextAPI(is_debug=is_debug)

    def process(self):
        d_final = {}
        for entity in self._filter_entities():
            cased_terms = self._extract_cased_terms(entity["summary"])
            d_final[entity["key"]] = {
                "terms": cased_terms,
                "summary": entity["summary"]}

        self.logger.debug("\n".join([
            "Case Term Extraction Completed"]))

        return d_final
