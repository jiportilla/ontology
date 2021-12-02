#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from datadict import FindSynonym
from nlusvc.core.bp import TextAPI
from nlusvc.displacy.dto import DisplacyEntityGenerator


class DisplacySpanExtractor(BaseObject):
    """
    """

    def __init__(self,
                 d_cluster: dict,
                 input_text: str,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            11-Oct-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-displacy-spans'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1093
        Updated:
            17-Oct-2019
            craig.trim@ibm.com
            *   replace has-match function with TextAPI version
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15377548
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   adjustment to synonym decomposition strategy
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1613#issuecomment-16624966
        Updated:
            13-Jan-2020
            craig.trim@ibm.com
            *   Replace code with Coordinate Extraction service
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1722
        :param d_cluster:
            a dictionary that clusters the tags around the chosen dimensions
            Sample Input:
                {   'activity':     ['activity'],
                    'agent':        [],
                    'anatomy':      ['natural killer cell'],
                    'artifact':     [],
                    'biology':      [],
                    'compound':     [],
                    'device':       [],
                    'disease':      [],
                    'organism':     [],
                    'other':        ['memory cd8 t cell', 'cytotoxicity'],
                    'pathology':    [],
                    'situation':    [],
                    'study':        [],
                    'tech':         []}
        :param input_text:
        :param ontology_name:
            the ontology name used to process the text
            e.g., 'biotext', 'base', etc
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_cluster = d_cluster
        self._input_text = input_text
        self._ontology_name = ontology_name

        self._text_api = TextAPI(is_debug=is_debug,
                                 ontology_name=ontology_name)

        self._synonym_finder = FindSynonym(is_debug=is_debug,
                                           ontology_name=ontology_name)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiated DisplacySpanExtractor",
                f"\tInput Text: {self._input_text}",
                f"\tOntology Name: {self._ontology_name}"]))

    def process(self) -> list:
        """
        :return:
            Sample Output
                [   {   'end': 26,
                        'label': 'ACTIVITY',
                        'start': 18,
                        'text': 'activity',
                        'type': 'tag'},
                    {   'end': 55,
                        'label': 'NATURAL KILLER CELL',
                        'start': 30,
                        'text': 'natural killer (nk) cells',
                        'type': 'inner-syn'},
                    {   'end': 17,
                        'label': 'CYTOTOXICITY',
                        'start': 8,
                        'text': 'cytotoxic',
                        'type': 'syn'},
                    {   'end': 70,
                        'label': 'MEMORY CD8 T CELL',
                        'start': 4,
                        'text': 'the cytotoxic activity of natural killer (nk) cells and memory cd8',
                        'type': 'inner-syn'} ]
        """
        entities = []

        input_text = self._input_text.lower()

        for key in self._d_cluster:
            for tag in self._d_cluster[key]:

                d_coords = self._text_api.coords(input_text=input_text,
                                                 entity_text=tag)

                if not d_coords:
                    continue

                entities.append(DisplacyEntityGenerator.generate(
                    text=tag,
                    entity_type='tag',
                    label=key,
                    start=d_coords['x'],
                    end=d_coords['y']))

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Displacy Span Extraction Complete",
                f"\tTotal Spans: {len(entities)}",
                f"\tOntology Name: {self._ontology_name}",
                f"{pprint.pformat(entities, indent=4)}"]))

        return entities
