#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datadict import FindEntity
from nlutext.core.dmo import CertificationConfidenceComputer
from nlutext.core.svc import PerformSupervisedParsing


class TextParser(BaseObject):
    """ parse incoming text data using both supervised and unsupervised techniques
        1.  supervised techniques use pre-defined dictionaries
            a.  typically generated out of Ontologies
            b.  typically higher precision
        2.  unsupervised techniques use ad-hoc language modeling
            a.  typically higher recall
    """

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            12-Mar-2019
            craig.trim@ibm.com
            the name of the activity within the manifest
        Updated:
            14-Mar-2019
            craig.trim@ibm.com
            *   renamed from 'svc:perform-supervised-parsing'
        Updated:
            2-Apr-2019
            craig.trim@ibm.com
            *   added enhanced debugging capabilities
            *   extensive modifications to '_create_power_action_words'
        Updated:
            25-Apr-2019
            craig.trim@ibm.com
            *   refactor out create-power-action-words into a service
        Updated:
            31-May-2019
            craig.trim@ibm.com
            *   introduce the notion of 'PUNCTUATIONSTOP' to reduce false-positive tag matching
                the prevents false-positive tag matching
        Updated:
            14-Jul-2019
            anassar@us.ibm.com
            *   it is necessary to attach th parser results to this object
        Updated:
            15-Jul-2019
            craig.trim@ibm.com
            *   add 'DataFrame' return capability to process method
        Updated:
            21-Aug-2019
            craig.trim@ibm.com
            *   deal with tag tuples and other refactoring
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
        Updated:
            6-Sept-2019
            craig.trim@ibm.com
            *   integrate confidence manipulation for self-certifications
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/878
        Updated:
            23-Sept-2019
            craig.trim@ibm.com
            *   remove unused and dead code
        Updated:
            28-Oct-2019
            craig.trim@ibm.com
            *   refactored 'pre-process' out of this business process
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1206
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)
        from nlutext.core.svc import NormalizeIncomingText

        self._results = dict()
        self._is_debug = is_debug
        self._ontology_name = ontology_name
        self._entity_finder = FindEntity(is_debug=is_debug,
                                         ontology_name=ontology_name)
        self._normalizer = NormalizeIncomingText(is_debug=is_debug,
                                                 ontology_name=ontology_name)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate TextParser",
                f"\tOntology Name: {ontology_name}"]))

    def results(self) -> dict:
        return self._results

    def _tags(self,
              original_ups: str,
              normalized_ups: str) -> dict:

        if not normalized_ups or len(normalized_ups) == 0:
            return {
                "supervised": [],
                "unsupervised": []}

        # Create Supervised Tag Set
        supervised_parser = PerformSupervisedParsing(is_debug=self._is_debug,
                                                     ontology_name=self._ontology_name)
        tags_1 = supervised_parser.process(original_ups, normalized_ups)

        # Remove Subsumed Tags
        subsumed = set()
        for t1 in tags_1:
            for t2 in tags_1:
                if t1[0] == t2[0]:
                    continue
                if t1[0] in t2[0]:
                    subsumed.add(t1[0])

        tags_2 = []
        for tag_tuple in tags_1:
            if tag_tuple[0] not in subsumed:
                tags_2.append(tag_tuple)

        tags_2 = CertificationConfidenceComputer(normalized_text=normalized_ups,
                                                 original_text=original_ups,
                                                 tag_tuples=tags_2,
                                                 is_debug=self._is_debug).process()

        return {
            "supervised": tags_2,
            "unsupervised": {}}

    @staticmethod
    def _postprocess(normalized_ups: str) -> str:
        normalized_ups = normalized_ups.replace("punctuationstop", "")
        normalized_ups = normalized_ups.replace("  ", " ")

        return normalized_ups

    def _to_result_set(self,
                       original_ups: str,
                       normalized_ups: str,
                       tags: dict,
                       as_dataframe: bool):

        if not as_dataframe:

            def _total_tags(tag_type: str):
                if tag_type in tags and tags[tag_type]:
                    return len(tags[tag_type])
                return 0

            total_tags = _total_tags("supervised") + _total_tags("unsupervised")

            return {
                "tags": tags,
                "total_tags": total_tags,
                "ups": {
                    "original": original_ups,
                    "normalized": normalized_ups}}

        results = []
        for tag_tuple in tags["supervised"]:
            tag_label = self._entity_finder.label_or_self(tag_tuple[0])
            results.append({
                "InputText": original_ups,
                "NormalizedText": normalized_ups,
                "Tag": tag_label,
                "Confidence": tag_tuple[1]})

        return pd.DataFrame(results)

    def process(self,
                original_ups: str,
                use_profiler: bool = False,
                as_dataframe: bool = False) -> dict or DataFrame:
        """

        :param original_ups:
        :param use_profiler:
        :param as_dataframe:
            False       return a dictionary (legacy)
            True        return a DataFrame  (modern)
        :return:
            as_dataframe = True
                +----+--------------+--------------------------+-------------------------+------------+
                |    |   Confidence | InputText                | NormalizedText          | Tag        |
                |----+--------------+--------------------------+-------------------------+------------|
                |  0 |           95 | Experience on key skills | experience on key skill | Experience |
                |  1 |           95 | Experience on key skills | experience on key skill | Skill      |
                +----+--------------+--------------------------+-------------------------+------------+
            as_dataframe = False
                {   'tags': {
                        'supervised': [
                            ('prostate', 90.6),
                            ('exocrine gland', 94.6)],
                        'unsupervised': {}},
                    'total_tags': 2,
                    'ups': {
                        'normalized': 'the exocrine_gland with prostate and redhat certified skills',
                        'original': 'the exocrine gland with prostate and redhat certified skills'}}
        """

        start = time.time()

        normalized_ups = self._normalizer.process(original_ups)["normalized"]

        if use_profiler:
            total_time = time.time() - start
            self.logger.debug(f"NormalizeIncomingText: {total_time}s")

        start = time.time()
        tags = self._tags(original_ups,
                          normalized_ups)

        if use_profiler:
            total_time = time.time() - start
            self.logger.debug(f"Tagging: {total_time}s")

        normalized_ups = self._postprocess(normalized_ups)
        self._results = self._to_result_set(original_ups=original_ups,
                                            normalized_ups=normalized_ups,
                                            tags=tags,
                                            as_dataframe=as_dataframe)
        return self._results
