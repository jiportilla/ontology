#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datadict import FindDimensions
from datadict import FindEntity


class TagTransformer(BaseObject):
    """
    Transform Tag Format

    sample input:
    [   '009712760': {
            'supervised': [ 'Communication',
                            'Infrastructure Specialist',
                            'Server',
                            'Solution'],
            'unsupervised': []
        },
        ... ]

    sample output:
    [   '009712760': {
            "tags": [
                {   "name":     "Deutsche Bank",
                    "type":     {
                        "schema": "agent",
                        "parent":   "bank" },
                    "provenance": {
                        "activity": "supervised",
                        "entities": ["source"]}},
        },
        ... ]
    """

    def __init__(self,
                 xdm_schema: str,
                 some_results: dict,
                 ontology_name:str='base',
                 is_debug: bool = False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
        Updated:
            29-Apr-2019
            craig.trim@ibm.com
            *   pass entity-schema as a param
        Updated:
            15-May-2019
            craig.trim@ibm.com
            *   added band information
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param some_results:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        if not some_results:
            raise MandatoryParamError("Results")

        self._is_debug = is_debug
        self._d_results = some_results
        self._entity_finder = FindEntity(is_debug=is_debug,
                                         ontology_name=ontology_name)
        self._dim_finder = FindDimensions(schema=xdm_schema,
                                          ontology_name=ontology_name,
                                          is_debug=is_debug)

    @staticmethod
    def transform(tag_name: str,
                  tag_type: str,
                  parent: str,
                  schema: str,
                  entities: list):
        """
        :param tag_name:
            the tag value
        :param tag_type:
        :param parent:
            the tag parent
            (e.g. 'DomainTerm' or 'Software')
        :param schema:
            the schema element
            this is a top-level element found in a file such as
                'entity_schema_for_dim.yml' (or similar)
        :param entities:
            a list of sources the tag was derived from
            most tags have ['source'] as an initial entity
            the names of the various relationships the entity was derived from are found here too
            for example
                ['source', 'rel-3', 'implies']
            indicates the tag was found in a transitive implication of three traversals from a source tag
        :return:
            a tag structure
        """
        return {"name": tag_name,
                "type": {
                    "schema": schema,
                    "parent": parent},
                "provenance": {
                    "activity": tag_type,
                    "entities": entities}}

    def process(self) -> dict:
        d_transformed = {}

        for key in self._d_results:

            l_tags = []
            for tag_type in ["supervised", "unsupervised", "system"]:

                for tag in self._d_results[key][tag_type]:
                    for schema in self._dim_finder.find(tag):
                        for parent in self._entity_finder.parents(tag):
                            l_tags.append(self.transform(tag_name=tag,
                                                         tag_type=tag_type,
                                                         parent=parent,
                                                         schema=schema,
                                                         entities=["source"]))
            d_transformed[key] = {
                "tags": l_tags,
                "attributes": self._d_results[key]["attributes"]}

        return d_transformed
