# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from datadict import FindDimensions


class ClusterTagsIntoDimensions(BaseObject):
    """ Cluster a list of tags (annotations) into a dimensionality schema

    Sample Input:
        [   'python', 'matplotlib', 'mapreduce', 'leadership', 'private cloud']

    Sample Output:
        {   'blockchain': [],
            'cloud': ['private cloud'],
            'data science': ['python', 'matplotlib', 'mapreduce'],
            'database': [],
            'hard skill': [],
            'other': ['leadership'],
            'project management': [],
            'quantum': [],
            'service management': [],
            'soft skill': [],
            'system administrator': []}
    """

    def __init__(self,
                 tags: list,
                 xdm_schema: str,
                 ontology_name: str,
                 is_debug: bool = True):
        """
        Created:
            19-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/970
        Updated:
            11-Oct-2019
            craig.trim@ibm.com
            *   check for 'unlisted' tags
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1092
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            14-Dec-2019
            craig.trim@ibm.com
            *   add ontology name as a parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1612
        :param tags:
            a list of tags to be organized into a dimensionality schema
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._tags = tags
        self._is_debug = is_debug

        self._dim_finder = FindDimensions(schema=xdm_schema,
                                          is_debug=is_debug,
                                          ontology_name=ontology_name)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiated ClusterTagsIntoDimensions",
                f"\tTags: {tags}",
                f"\tOntology Name: {ontology_name}",
                f"\tSchema: {xdm_schema}"]))

    def process(self):
        master = {}

        for entity in self._dim_finder.top_level_entities():
            master[entity] = []

        if self._is_debug:
            self.logger.debug(f"Instantiated Schema: "
                              f"{pprint.pformat(master)}")

        for tag in self._tags:
            for schema in self._dim_finder.find(tag):

                if schema.lower() == "unlisted":
                    schema = "other"
                elif schema not in master:
                    self.logger.warning(f"Schema Not Found "
                                        f"(schema={schema})")
                    continue

                master[schema].append(tag)
                if self._is_debug:
                    self.logger.debug(f"Tag Located "
                                      f"(tag={tag}, cluster={schema})")

        if self._is_debug:
            self.logger.debug(f"Completed Schema: "
                              f"{pprint.pformat(master)}")

        return master
