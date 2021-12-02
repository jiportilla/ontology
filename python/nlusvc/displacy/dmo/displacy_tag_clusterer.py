#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from cendantdim import DimensionsAPI
from datamongo import BaseMongoClient


class DisplacyTagClusterer(BaseObject):
    """
    Sample Input (annotations):
        [   (redhat, 97.3, base),
            (exocrine gland, 98.3, biotech) ]

    Sample Output:
        {   'blockchain':           [],
            'quantum':              [],
            'cloud':                [],
            'system administrator': ['redhat'],
            'database':             [],
            'data science':         [],
            'hard skill':           [],
            'other':                [],
            'soft skill':           [],
            'project management':   [],
            'service management':   [] }

    """

    def __init__(self,
                 tags: list,
                 xdm_schema: str,
                 ontology_name: str,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = False):
        """
        Created:
            14-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-display-spans' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1594
            *   add ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1612
        :param tags:
            the list of tag tuples generated in the prior step
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param mongo_client:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._tags = tags
        self._is_debug = is_debug
        self._xdm_schema = xdm_schema
        self._mongo_client = mongo_client
        self._ontology_name = ontology_name

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate DisplacyTagCluster",
                f"\tTags: {tags}",
                f"\tSchema: {xdm_schema}",
                f"\tOntology Name: {ontology_name}"]))

    def process(self,
                tag_confidence_threshold: float = 0.0) -> dict:
        """
        :param tag_confidence_threshold:
            Reference GIT-1613-16624815 for value strategies
        :return:
        """
        dim_api = DimensionsAPI(mongo_client=self._mongo_client,
                                is_debug=self._is_debug)

        _tags = [x[0] for x in self._tags
                 if x[1] > tag_confidence_threshold]

        svcresult = dim_api.cluster(tags=_tags,
                                    xdm_schema=self._xdm_schema,
                                    ontology_name=self._ontology_name)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Displacy Tag Clustering Complete",
                f"\tTotal Tags: {len(svcresult)}",
                f"\tXDM Schema: {self._xdm_schema}",
                f"\tOntology Name: {self._ontology_name}",
                f"{pprint.pformat(svcresult)}"]))

        return svcresult
