#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datadict import FindDimensions


class TransformDimensions(BaseObject):
    """ transform the Dimemsionality Map into a Simplified, Weighted Map

        sample output:
        dimensions = [
            {   "key": {
                    "type": "serial_number",
                    "value": "12345",
                    "source": <source> },
                rels: [
                    "dim-1": 4,
                    "dim-2": 3,
                    ...
                    "dim-n": 8 }
            },
            ...
        ]
    """

    def __init__(self,
                 d_results: dict,
                 xdm_schema: str,
                 is_debug: bool = False):
        """
        Created:
            25-Mar-2019
            craig.trim@ibm.com
        Updated:
            29-Mar-2019
            craig.trim@ibm.com
            *   renamed from 'transform-dim-map'
        Updated:
            25-Apr-2019
            craig.trim@ibm.com
            *   added 'dimemsionality-schema' as input
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param d_results:
            sample input:
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
        self._dim_finder = FindDimensions(xdm_schema)

    def _init_slots(self) -> dict:
        d_slots = {}

        entities = [x for x in self._dim_finder.top_level_entities()
                    if x != "other"]
        for entity in entities:
            d_slots[entity] = {
                "value": 0.0,
                "weight": 0.0}

        return d_slots

    def _group_by_schema(self) -> dict:
        all_schemas = {}

        for keyfield in self._d_results:

            d_schema = {}
            tags = self._d_results[keyfield]["tags"]

            for tag in tags:
                schema = tag["type"]["schema"]

                if schema not in d_schema:
                    d_schema[schema] = {}
                d_schema[schema][tag["name"]] = tag

            all_schemas[keyfield] = d_schema

        return all_schemas

    @staticmethod
    def _weighted_value(tags: list):
        from cendantdim.batch.dmo import RelationshipWeighter

        combined_weight = 0.0
        for tag in tags:
            provenance = dict(tags[tag])["provenance"]
            combined_weight += RelationshipWeighter(provenance).process()

        return combined_weight

    def _weighting(self,
                   all_schemas: dict) -> list:
        from cendantdim.batch.dmo import DimensionCalculator

        results = []
        for keyfield in all_schemas:
            d_schema = all_schemas[keyfield]

            slots = self._init_slots()

            schema_elements = [x for x in d_schema
                               if x != "other"]

            for schema_element in schema_elements:
                weighted_value = self._weighted_value(d_schema[schema_element])
                slots[schema_element]["value"] = weighted_value

            _weights = DimensionCalculator(slots,
                                           is_debug=self._is_debug).process()

            keys = list(slots.keys())
            for i in range(0, len(keys)):
                slots[keys[i]]["weight"] = _weights[i]

            results.append({
                "key_field": keyfield,
                "slots": slots})

        return results

    def process(self) -> list:

        # Step: Aggregate results by Schema Element
        all_schemas = self._group_by_schema()

        results = self._weighting(all_schemas)

        return results
