#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging

from base import FileIO

logger = logging.getLogger(__name__)


class KbResources(object):
    """
    Purpose:
        *   paths for resources/
            these are input files used the MDA.sh process
    """

    def __init__(self, validate=True):
        self.validate = validate

    def get_path(self, some_file_name):
        return FileIO.absolute_path(some_file_name, self.validate)

    def flow_mapping_modified(self):
        return self.get_path("nlu/auto_generated/flow_mapping_kb_modified.csv")

    def params_modified(self):
        return self.get_path("nlu/auto_generated/params_kb_modified.csv")

    def entities_modified(self):
        return self.get_path("nlu/auto_generated/entity_kb_modified.csv")

    def entity_inference(self):
        return self.get_path("nlu/entities/entities_inference.yml")

    def entities(self):
        return self.get_path("nlu/entities/entities_kb.yml")

    def entities_manual_(self):
        return self.get_path("nlu/entities/entities__manual_kb.yml")

    def entities_auto_(self):
        return self.get_path("nlu/entities/entities__auto_kb.yml")

    def entities_cities(self):
        return self.get_path("nlu/entities/entities_cities_kb.yml")

    def entities_numbers(self):
        return self.get_path("nlu/entities/entities_numbers_kb.yml")

    def flow_mapping(self):
        return self.get_path("nlu/flows/flow_mapping_kb.csv")

    def flow_taxonomy(self):
        return self.get_path("nlu/flows/flow_taxonomy_kb.csv")

    def siblings(self):
        return self.get_path("nlu/rels/siblings_kb.csv")

    # def synonyms(self):
    #     return self.get_path("nlu/rels/synonyms_kb.csv")

    # def synonyms_numbers(self):
    #     return self.get_path("nlu/rels/synonyms_numbers_kb.csv")

    def false_positives(self):
        return self.get_path("nlu/rels/falsepositives_kb.csv")

    def antonyms(self):
        return self.get_path("nlu/rels/antonyms_kb.csv")

    def stop_words(self):
        return self.get_path("nlu/other/stopwords_kb.csv")

    def templates(self):
        return self.get_path("nlu/other/nlg_templates_kb.yml")

    def urls(self):
        return self.get_path("nlu/other/urls_kb.yml")

    def mapping_att(self):
        return self.get_path("nlu/flows/att_mapping_kb.yml")
