#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import itertools
import pprint

from base import BaseObject
from base import DataTypeError
from datadict import FindDimensions
from datamongo import BaseMongoClient
from nlusvc.displacy.dmo import DisplacyNerGenerator
from nlusvc.displacy.dmo import DisplacySpanConfidence
from nlusvc.displacy.dmo import DisplacySpanExtractor
from nlusvc.displacy.dmo import DisplacySpanMerger
from nlusvc.displacy.dmo import DisplacySpanSorter
from nlusvc.displacy.dmo import DisplacySubsumedFilter
from nlusvc.displacy.dmo import DisplacyTagClusterer
from nlusvc.displacy.dmo import DisplacyTagGenerator


class GenerateDisplacySpans(BaseObject):
    """ spaCy is an open-source NLP engine that comes with a built-in entity visualizer (displacy)
        this function does not train a spaCy model, but it will use Cendant to annotate text
        and compute the spans across the text

   Sample Input:
        Experience on key skills like Solution Manager Datastage SAP Pack ABAP Accelerators BAPI Smartforms Adobe
        forms SAP SD and SAP CRM SAP PI EWM Worked on advanced technology components like ALE EDI Workflow and
        LSMW Migration SAP Solution Manager Strong database skills Object Oriented Programming and development
        knowledge Extensive work on Data Dictionary Objects( Tables Structures Domains Data elements
        Views Lock objects) Involved in 2 implementation 1 up gradation and 3 support projects.

    Sample Tags (annotations):
        [   ['abap for sap hana 2.0', 94],
            ['data migration', 64],
            ['data skill', 0],
            ['data structure', 88],
            ['database skill', 65.6],
            ['domain skill', 0],
            ['experience', 33.6],
            ['implement', 33.6],
            ['infosphere datastage', 65.6],
            ['migrate', 33.6],
            ['object oriented', 65.6],
            ['performance appraisal', 100],
            ['programming skill', 65.6],
            ['project', 33.6],
            ['sap pi', 65.6],
            ['sap sd', 65.6],
            ['sap solution manager', 76.3],
            ['skill migration', 16],
            ['support', 33.6],
            ['upgrade', 33.6],
            ['workflow', 33.6]  ]

    Sample Dimensionality Cluster:
        {   'blockchain': [],
            'quantum': [],
            'cloud': [],
            'system administrator': ['data migration'],
            'database': ['data structure', 'database skill', 'infosphere datastage'],
            'data science': [],
            'hard skill': ['abap for sap hana 2.0', 'implement', 'object oriented', 'programming skill'],
            'other': ['migrate'],
            'soft skill': ['experience', 'performance appraisal', 'project', 'workflow'],
            'project management': [],
            'service management': ['sap pi', 'sap sd', 'sap solution manager', 'support', 'upgrade'] }

    Sample Output:
        [   {'start': 0, 'end': 10, 'label': 'SOFT SKILL'},
            {'start': 47, 'end': 56, 'label': 'DATABASE'},
            {'start': 112, 'end': 118, 'label': 'SERVICE MANAGEMENT'},
            {'start': 131, 'end': 137, 'label': 'SERVICE MANAGEMENT'},
            {'start': 196, 'end': 204, 'label': 'SOFT SKILL'},
            {'start': 214, 'end': 223, 'label': 'OTHER'},
            {'start': 224, 'end': 244, 'label': 'SERVICE MANAGEMENT'},
            {'start': 268, 'end': 283, 'label': 'HARD SKILL'},
            {'start': 284, 'end': 295, 'label': 'HARD SKILL'},
            {'start': 439, 'end': 453, 'label': 'HARD SKILL'},
            {'start': 456, 'end': 468, 'label': 'SERVICE MANAGEMENT'},
            {'start': 475, 'end': 482, 'label': 'SERVICE MANAGEMENT'}   ]
    """

    def __init__(self,
                 input_text: str,
                 xdm_schema: str,
                 mongo_client: BaseMongoClient,
                 ontology_names: list,
                 is_debug: bool = False):
        """
        Created:
            20-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/972
        Updated:
            11-Oct-2019
            craig.trim@ibm.com
            *   updated logging
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1092
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            18-Nov-2019
            craig.trim@ibm.com
            *   add transformation for input-text in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1399
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   defect fix in displacy span generation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1595
            *   use multiple ontology names in params
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1597#issuecomment-16622685
            *   perform refactoring of logic into multiple domain components
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1597#issuecomment-16623626
        :param input_text:
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param mongo_client:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        if type(ontology_names) != list:
            raise DataTypeError("Ontology Names, list")

        self._is_debug = is_debug
        self._xdm_schema = xdm_schema
        self._mongo_client = mongo_client
        self._ontology_names = ontology_names
        self._input_text = self._transform(input_text)

        self._d_dim_finders = self._init_dim_finders()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Initialized GenerateDisplacySpans",
                f"\tOntology Names: {ontology_names}",
                f"\tInput Text: {self._input_text}"]))

    def _init_dim_finders(self) -> dict:
        d_finders = {}
        for ontology_name in self._ontology_names:
            d_finders[ontology_name] = FindDimensions(schema=self._xdm_schema,
                                                      is_debug=self._is_debug,
                                                      ontology_name=ontology_name)
        return d_finders

    @staticmethod
    def _transform(input_text: str) -> str:
        input_text = input_text.replace(".", " . ")

        return input_text

    def process(self,
                title: str = None,
                use_schema_elements: bool = True,
                span_confidence_threshold: float = 0.50) -> list:
        """
        Purpose:
            Create a Data Structure useful for Displacy Span Rendering
        :param use_schema_elements:
            True        Use Schema Elements instead of Cendant Ontology Tags
                        Sample Output:
                        {   'end': 39,
                            'label': 'DATA SCIENCE',
                            'start': 23,
                            'text': 'Machine Learning',
                            'type': 'tag'}
            False       Use Cendant Ontology Tags instead of Schema Elements
                        Sample Output:
                        {   'end': 39,
                            'label': 'MACHINE LEARNING',
                            'start': 23,
                            'text': 'Machine Learning',
                            'type': 'tag'}
        :param title:
            the title of the analysis (optional)
        :param span_confidence_threshold:
            the threshold of span confidence required to maintain an entity for displaCy
            ref GIT-1803-17531208
        :return:
            the displacy spans list
        """

        d_entities = {}
        d_normalized = {}
        for ontology_name in self._ontology_names:
            tags, normalized_text = DisplacyTagGenerator(is_debug=self._is_debug,
                                                         input_text=self._input_text,
                                                         ontology_names=[ontology_name]).process()

            d_normalized[ontology_name] = normalized_text

            d_cluster = DisplacyTagClusterer(tags=tags,
                                             is_debug=self._is_debug,
                                             xdm_schema=self._xdm_schema,
                                             ontology_name=ontology_name,
                                             mongo_client=self._mongo_client).process()

            entities = DisplacySpanExtractor(d_cluster=d_cluster,
                                             is_debug=self._is_debug,
                                             input_text=self._input_text,
                                             ontology_name=ontology_name).process()

            entities = DisplacySpanMerger(is_debug=self._is_debug,
                                          input_entities=entities).process()

            for entity in entities:
                entity['ontology'] = ontology_name

            if use_schema_elements:
                def _finder() -> FindDimensions:
                    return self._d_dim_finders[ontology_name]

                for entity in entities:
                    entity["label"] = _finder().find(entity["label"])[0]

            d_entities[ontology_name] = entities

        entities = list(itertools.chain.from_iterable(d_entities.values()))

        entities += DisplacyNerGenerator(input_text=self._input_text,
                                         is_debug=self._is_debug).process()

        entities = DisplacySpanSorter(is_debug=self._is_debug,
                                      input_entities=entities).process()

        entities = DisplacySpanConfidence(is_debug=self._is_debug,
                                          input_entities=entities).process()

        entities = [entity for entity in entities
                    if entity['span_confidence'] > span_confidence_threshold]

        entities = DisplacySubsumedFilter(is_debug=self._is_debug,
                                          input_entities=entities).process()

        # Data Structure Ref: GIT-1597-16624188
        svcresult = [{"text": self._input_text,
                      "normalized": d_normalized,
                      "ents": entities,
                      "title": title}]

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Entity Span Generation Completed",
                f"\tInput Text: {self._input_text}",
                f"\tOntology Names: {self._ontology_names}",
                f"{pprint.pformat(svcresult)}"]))

        return svcresult
