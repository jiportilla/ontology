#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datadict import FindDimensions
from datadict import FindEntity


class GenerateDimensions(BaseObject):
    """ this service generates a Dimensionality Map from the Skills Dictionary

        based on the output of the prior service (transform-skill-graph)
        this services generates the following output:
    """

    __invalid_parents = ["domainterm", "root"]

    def __init__(self,
                 d_results: dict,
                 xdm_schema: str,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            25-Mar-2019
            craig.trim@ibm.com
        Updated:
            29-Mar-2019
            craig.trim@ibm.com
            *   renamed from 'generate-dim-map'
        Updated:
            11-Apr-2019
            craig.trim@ibm.com
            *   defect fixing
        Updated:
            25-Apr-2019
            craig.trim@ibm.com
            *   added 'dimensionality_schema' parameter
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
        :param d_results:
            the dimensionality input
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        if not d_results:
            raise MandatoryParamError("Input Dictionary")

        self._is_debug = is_debug
        self._d_results = d_results
        self._ontology_name = ontology_name

        self._entity_finder = FindEntity(is_debug=is_debug,
                                         ontology_name=ontology_name)
        self._dim_finder = FindDimensions(schema=xdm_schema,
                                          is_debug=is_debug,
                                          ontology_name=ontology_name)

    def _generate_tag_structures(self,
                                 tags: list):
        """
        generate formal tag structures from a list of tags
        :param tags:
        :return:
        """
        from cendantdim import TagTransformer
        from . import ExtractExtendedRelationships

        d_new = {}
        for tag in tags:

            svcresult = ExtractExtendedRelationships(some_tags=[tag],
                                                     is_debug=self._is_debug,
                                                     ontology_name=self._ontology_name).process()

            for rel_graph_key in svcresult["iterations"]:  # e.g. rel-1, rel-2, etc

                _graph_key = dict(svcresult["iterations"][rel_graph_key])

                if "relationships" not in _graph_key:
                    continue

                relationships = _graph_key["relationships"]
                for rel_name in relationships:  # e.g. implies, owns, etc

                    for tag_name in sorted(set(relationships[rel_name])):

                        _parent = self._entity_finder.first_parent(tag_name,
                                                                   raise_warning=False,
                                                                   raise_value_error=False)
                        if _parent.lower().strip() == "root":
                            continue

                        _tag_type = tag["provenance"]["activity"]
                        for schema in self._dim_finder.find(tag_name):
                            _entities = tag["provenance"]["entities"] + [rel_name, rel_graph_key]

                            _new_tag = TagTransformer.transform(tag_name=tag_name,
                                                                tag_type=_tag_type,
                                                                parent=_parent,
                                                                schema=schema,
                                                                entities=_entities)
                            d_new[tag_name] = _new_tag

        return d_new

    def _get_tags(self) -> None:
        """ generate new tag structures from existing tag relationships """
        for key in self._d_results:
            tags = self._d_results[key]["tags"]
            d_new = self._generate_tag_structures(tags)
            self._d_results[key]["tags"] = tags + list(d_new.values())

    def process(self) -> dict:
        self._get_tags()
        return self._d_results
