#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GenerateOntologyDictionaries(BaseObject):
    """ Generates all dictionaries for a given Ontology """

    def __init__(self,
                 ontology_name: str,
                 syns_only: bool = False,
                 is_debug: bool = False):
        """
        Created:
            13-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'mda-orchestrator'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
        Updated:
            14-Jan-2020
            craig.trim@ibm.com
            *   ensure synonym generation uses see-also dictionary
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1734
        :param ontology_name:
            the name of the Ontology (e.g., 'base' or 'biotech')
        :param syns_only:
            True        only generate synonym dictionaries
            False       generate all dictionaries
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._syns_only = syns_only
        self._ontology_name = ontology_name

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Initialize Ontology Generation",
                f"\tSyns Only? {self._syns_only}",
                f"\tOntology Name: {self._ontology_name}"]))

    def _generate_syn_dictionaries(self) -> dict:
        from taskmda import GenerateSynonyms
        from taskmda import GenerateSeeAlso
        from taskmda import GeneratePatterns

        see_also_generation = GenerateSeeAlso(is_debug=self._is_debug,
                                              ontology_name=self._ontology_name)
        pattern_generation = GeneratePatterns(is_debug=self._is_debug,
                                              ontology_name=self._ontology_name)
        synonym_generation = GenerateSynonyms(is_debug=self._is_debug,
                                              ontology_name=self._ontology_name)

        # the 'injected synonyms' dictionary
        # these do NOT come from the synonyms file
        d_synonyms = {}  # GIT-1734-17146514

        def _merge(some_dict: dict) -> None:
            for k in some_dict:
                if k not in d_synonyms:
                    d_synonyms[k] = []
                [d_synonyms[k].append(x) for x in some_dict[k]]

        _merge(see_also_generation.process())
        _merge(pattern_generation.process()["synonyms"])

        synonym_generation.process(d_synonyms)

        return d_synonyms

    def _generate_dictionaries(self,
                               d_patterns: dict) -> None:
        from taskmda import GenerateRelationships
        from taskmda import GenerateLabels
        from taskmda import GenerateParents
        from taskmda import GenerateEntityNgrams
        from taskmda import GenerateMetrics
        from taskmda import GenerateSeeAlso

        parents_gen = GenerateParents(is_debug=self._is_debug,
                                      ontology_name=self._ontology_name)
        ontology_gen = GenerateRelationships(is_debug=self._is_debug,
                                             ontology_name=self._ontology_name)
        label_gen = GenerateLabels(is_debug=self._is_debug,
                                   ontology_name=self._ontology_name)
        metrics_gen = GenerateMetrics(is_debug=self._is_debug,
                                      ontology_name=self._ontology_name)

        see_also = GenerateSeeAlso(is_debug=self._is_debug,
                                   ontology_name=self._ontology_name)

        parents_gen.process()
        ontology_gen.process()
        labels = label_gen.process()
        see_also.process()

        GenerateEntityNgrams(some_labels=labels,
                             is_debug=self._is_debug,
                             some_patterns=d_patterns,
                             ontology_name=self._ontology_name).process()

        metrics_gen.process()

    def process(self) -> None:
        """
        Purpose
            Generate Dictionaries from an Ontology
        """
        d_patterns = self._generate_syn_dictionaries()
        if not self._syns_only:
            self._generate_dictionaries(d_patterns)
