#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from nlutext import PerformSentenceSegmentation
from nlutext import TextParser


class DBpediaSummaryTagger(BaseObject):
    """ for each DBpedia entry, extract the summary and perform entity tagging
    """

    def __init__(self,
                 d_entities: dict):
        """
        Updated:
            23-Apr-2019
            craig.trim@ibm.com
            *   refactored out of script 'wikipedia-entity-analysis-02'
        :param d_entities:
            the dictionary generated from 'dbpedia-entity-lookup'
        """
        BaseObject.__init__(self, __name__)
        self.d_entities = d_entities

        self._parser = TextParser()
        self._segmenter = PerformSentenceSegmentation()

    def _sentences(self,
                   summary: str) -> list:
        """
        Purpose:
            Transform a DBpedia summary section into a list of sentences
        :param summary:
            a DBpedia summary section
        :return:
            a list of sentences
        """

        def _sentences() -> list:
            return [x.strip() for x in summary.split("\n") if x]

        sentences = []
        for line in _sentences():
            [sentences.append(x) for x in self._segmenter.process(line)]

        sentences = [str(x).strip() for x in sentences if x]

        return sentences

    def _handle_entity(self,
                       some_entity: dict):

        summary = some_entity["summary"]
        sentences = self._sentences(summary)

        s = set()
        svcresults = []

        for line in sentences:
            svcresult = self._parser.process(line)
            [s.add(x) for x in svcresult["tags"]["supervised"]]
            svcresults.append(svcresult)

        return {
            "tags": sorted(s),
            "svcresults": svcresults,
            "title": some_entity["key"]}

    def _filter_entities(self) -> list:
        """
        :return:
            a list of entities with a 'summary' section
        """
        entities = []
        for k in self.d_entities:
            if "summary" in self.d_entities[k]:

                # temp - until next run complete
                entity = self.d_entities[k]
                if "key" not in entity:
                    entity["key"] = k
                # end temp

                entities.append(entity)
        return entities

    def process(self) -> list:
        results = []

        for entity in self._filter_entities():
            results.append(self._handle_entity(
                entity))

        self.logger.debug("\n".join([
            "DBpedia Summary Tagging Complete (total={})".format(
                len(results))]))

        return results
