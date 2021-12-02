#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class MultiTextParser(BaseObject):
    """ Parse Incoming Text using 1..* Ontologies
    """

    def __init__(self,
                 ontology_names: list,
                 is_debug: bool = False):
        """
        Created:
            13-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1590#issuecomment-16614898
        :param ontology_names:
            an ordered list of Ontology Names
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_names = ontology_names

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate MultiTextParser",
                f"\tOntology Names: {ontology_names}"]))

    @staticmethod
    def _to_dict(input_text: str,
                 normalized_text: str,
                 d_supervised_tags: dict) -> dict:
        """
        Sample Output:
            {   'tags':
                {   'supervised': [
                        ('redhat', {'confidence': 97.3, 'ontology': 'base'}),
                        ('exocrine gland', {'confidence': 98.3, 'ontology': 'biotech'})]},
                'total_tags': 2,
                'ups': {
                    'normalized': 'exocrine_gland redhat',
                    'original': 'exocrine gland red hat'}}
        :param input_text:
        :param normalized_text:
        :param d_supervised_tags:
        """

        def to_tuples() -> list:
            results = []
            for k in d_supervised_tags:
                results.append((k, d_supervised_tags[k]))
            return results

        return {
            'tags': {
                'supervised': to_tuples(),
                'unsupervised': {}},
            'total_tags': len(d_supervised_tags),
            'ups': {
                'normalized': normalized_text,
                'original': input_text}}

    @staticmethod
    def _to_dataframe(input_text: str,
                      normalized_text: str,
                      d_supervised_tags: dict) -> DataFrame:
        """
        Sample Output:
            +----+--------------+------------------------+-----------------------+------------+----------------+
            |    |   Confidence | InputText              | NormalizedText        | Ontology   | Tag            |
            |----+--------------+------------------------+-----------------------+------------+----------------|
            |  0 |         97.3 | exocrine gland red hat | exocrine_gland redhat | base       | redhat         |
            |  1 |         98.3 | exocrine gland red hat | exocrine_gland redhat | biotech    | exocrine gland |
            +----+--------------+------------------------+-----------------------+------------+----------------+
        :param input_text:
        :param normalized_text:
        :param d_supervised_tags:
        """
        results = []

        for key in d_supervised_tags:
            results.append({
                "Tag": key,
                "InputText": input_text,
                "NormalizedText": normalized_text,
                "Ontology": d_supervised_tags[key]["ontology"],
                "Confidence": d_supervised_tags[key]["confidence"]})

        return pd.DataFrame(results)

    def process(self,
                original_ups: str,
                use_profiler: bool = False,
                as_dataframe: bool = False) -> dict or DataFrame:
        from nlutext.core.bp import TextParser

        d_supervised_tags = {}
        normalized_text = original_ups

        for ontology_name in self._ontology_names:
            text_parser = TextParser(is_debug=self._is_debug,
                                     ontology_name=ontology_name)

            svcresult = text_parser.process(as_dataframe=False,
                                            use_profiler=use_profiler,
                                            original_ups=normalized_text)

            # Step: Update Normalized Text
            normalized_text = svcresult["ups"]["normalized"]

            # Step: Update Tag Dictionary
            for tag in svcresult["tags"]["supervised"]:
                if tag[0] in d_supervised_tags:  # tag exists ...
                    if d_supervised_tags[tag[0]]["confidence"] < tag[1]:  # ... and has lower confidence
                        d_supervised_tags[tag[0]]["confidence"] = tag[1]  # update confidence
                else:  # tag does not exist
                    d_supervised_tags[tag[0]] = {"confidence": tag[1],
                                                 "ontology": ontology_name}

        if not as_dataframe:
            return self._to_dict(input_text=original_ups,
                                 normalized_text=normalized_text,
                                 d_supervised_tags=d_supervised_tags)

        return self._to_dataframe(input_text=original_ups,
                                  normalized_text=normalized_text,
                                  d_supervised_tags=d_supervised_tags)
