#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
import time

from base import BaseObject
from datadict import DictionaryLoader
from datamongo import CendantCollection


class GenerateMetrics(BaseObject):
    """ Generate Metrics """

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            4-Apr-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/60
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   major refactoring from the ground-up in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583#issuecomment-16612838
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._ontology_name = ontology_name
        self._dict_loader = DictionaryLoader(is_debug=self._is_debug,
                                             ontology_name=self._ontology_name)

        self.collection = self._collection()

    @staticmethod
    def _collection():
        return CendantCollection(some_db_name="cendant",
                                 some_collection_name="metrics_mda")

    @staticmethod
    def _count_keys_values(d: dict) -> dict:
        total_keys = len(d)
        total_values = 0
        for k in d:
            if type(d[k]) == list:
                total_values += len(d[k])
            elif type(d[k]) == dict:
                total_values += sum([len(d[k][x]) for x in d])

        return {"keys": total_keys,
                "values": total_values}

    @staticmethod
    def _count_keys(d: dict) -> dict:
        total_keys = len(d)
        return {"keys": total_keys}

    def process(self):
        label_count = self._count_keys(self._dict_loader.taxonomy().labels())
        ngram_count = self._count_keys_values(self._dict_loader.taxonomy().ngrams())
        parent_count = self._count_keys_values(self._dict_loader.taxonomy().parents())
        pattern_count = self._count_keys_values(self._dict_loader.taxonomy().patterns())
        defines_count = self._count_keys_values(self._dict_loader.relationships().defines())
        implies_count = self._count_keys_values(self._dict_loader.relationships().implies())
        infinitive_count = self._count_keys_values(self._dict_loader.relationships().infinitive())
        owns_count = self._count_keys_values(self._dict_loader.relationships().owns())
        parts_count = self._count_keys_values(self._dict_loader.relationships().parts())
        requires_count = self._count_keys_values(self._dict_loader.relationships().requires())
        similarity_count = self._count_keys_values(self._dict_loader.relationships().similarity())
        versions_count = self._count_keys_values(self._dict_loader.relationships().versions())

        svcresult = {
            "tts": str(time.time()),
            "owl": self._ontology_name,
            "entities": {
                "labels": label_count,
                "ngrams": ngram_count,
                "parents": parent_count,
                "patterns": pattern_count},
            "rels": {
                "defines": defines_count,
                "implies": implies_count,
                "infinitive": infinitive_count,
                "owns": owns_count,
                "parts": parts_count,
                "requires": requires_count,
                "similarity": similarity_count,
                "versions": versions_count}}

        self.logger.info("\n".join([
            "Generated MDA Metrics",
            pprint.pformat(svcresult, indent=4)]))

        self._collection().save(svcresult)
