#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

import spacy

from base import BaseObject


class DisplacyNerGenerator(BaseObject):
    """ Annotate the Input Text with the spaCy NER model

    Sample Input:
        The investment arm of biotech giant Amgen has led a new $6 million
        round of funding in GNS Healthcare, CEO Colin Hill tells Xconomy.

    Sample Output:
        [   {   'label': 'Company',
                'start': 36,
                'end': 41,
                'text': 'Amgen',
                'type': 'ner'},
            {   'label': 'Money',
                'start': 56,
                'end': 66,
                'text': '$6 million',
                'type': 'ner'},
            {   'label': 'Company',
                'start': 87,
                'end': 101,
                'text': 'GNS Healthcare',
                'type': 'ner'},
            {   'label': 'Person',
                'start': 107,
                'end': 117,
                'text': 'Colin Hill',
                'type': 'ner'},
            {   'label': 'GPE',
                'start': 124,
                'end': 131,
                'text': 'Xconomy',
                'type': 'ner'}]
    """

    __nlp = spacy.load("en_core_web_sm")

    __label_transform = {
        'ORG': 'Company'
    }

    def __init__(self,
                 input_text: str,
                 is_debug: bool = False):
        """
        Created:
            3-Feb-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1810
        :param input_text:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._input_text = input_text

    def _generate(self,
                  ent) -> dict:
        from nlusvc.displacy.dto import DisplacyEntityGenerator

        def label() -> str:
            if ent.label_ in self.__label_transform:
                return self.__label_transform[ent.label_].lower()
            return ent.label_.lower()

        entity = DisplacyEntityGenerator.generate(text=ent.text,
                                                  entity_type='tag',
                                                  start=ent.start_char,
                                                  end=ent.end_char,
                                                  label=label())

        entity['ontology'] = 'en_core_web_sm'
        return entity

    def process(self) -> list:
        doc = self.__nlp(self._input_text)

        entities = [self._generate(ent) for ent in doc.ents]

        if self._is_debug:
            self.logger.debug('\n'.join([
                "NER Generation Complete",
                pprint.pformat(entities, indent=4)]))

        return entities
