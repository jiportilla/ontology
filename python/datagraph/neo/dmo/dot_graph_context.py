#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import hashlib
import pprint

from base import BaseObject
from base import MandatoryParamError
from datadict import FindDimensions


class DotGraphContext(BaseObject):
    """ """

    def __init__(self,
                 xdm_schema: str = 'supply'):
        """
        Created:
            7-Apr-2019
            craig.trim@ibm.com
            *   based on 'neo-graph-context'
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        """
        BaseObject.__init__(self, __name__)
        self._node_lookup = {}
        self._predicate_lookup = {}
        self._dim_finder = FindDimensions(xdm_schema)

    def node_lookup(self):
        return self._node_lookup

    def predicate_lookup(self):
        return self._predicate_lookup

    def total_nodes(self) -> int:
        return len(self._node_lookup)

    @staticmethod
    def _validate(some_name: str,
                  some_parent: str):
        if not some_name or type(some_name) != str:
            raise MandatoryParamError("DataType Failure (Name)")
        if not some_parent or type(some_parent) != str:
            raise MandatoryParamError("DataType Failure (Parent)")

    @staticmethod
    def _loaded_on() -> str:
        from . import DotUtils
        return DotUtils.loaded_on()

    @staticmethod
    def _md5(some_value: str) -> str:
        some_value = str(some_value).upper().strip()
        return hashlib.md5(some_value.encode()).hexdigest()

    @staticmethod
    def _key(*args):
        return "-".join(list(args))

    @staticmethod
    def _add_manifest_properties(manifest_properties: dict) -> dict:
        """ a manifest property looks like this:

            sample input (YML Manifest):
                - source_name: geography
                  target_name: Geography
                  properties:
                    - region: region

            sample output:
                { region: <value> }
        """
        d_manifest = {}
        if manifest_properties:
            for p in manifest_properties:
                d_manifest[p["name"]] = p["value"]
        return d_manifest

    def _add_provenance_properties(self,
                                   provenance_properties: dict) -> dict:
        """
        """
        d_provenance = {"date": self._loaded_on()}
        if provenance_properties:
            for k in provenance_properties:
                d_provenance[k] = provenance_properties[k]
        return d_provenance

    def find_or_create_relationships(self,
                                     subject_name: str,
                                     predicate_name: str,
                                     object_name: str,
                                     provenance: dict,
                                     is_debug=False):
        """ find a node in cache or create and add to cache """

        _key = self._md5(self._key(subject_name,
                                   predicate_name,
                                   object_name))

        # return node from cache
        if _key in self._predicate_lookup:
            return self._predicate_lookup[_key]

        _triple = {
            "id": _key,
            "triple": {
                "subject": subject_name,
                "predicate": predicate_name,
                "object": object_name},
            "manifest": {},
            "provenance": self._add_provenance_properties(provenance)}

        self._predicate_lookup[_key] = _triple

        if is_debug:
            self.logger.debug("\n".join([
                "Generated Triple",
                pprint.pformat(_triple, indent=4)]))

        return _key

    def find_or_create_node(self,
                            node_name: str,
                            node_parent: str,
                            manifest: dict,
                            provenance: dict,
                            is_debug=False) -> dict:
        """ find a node in cache or create and add to cache """

        # Step: Validate Primary Properties
        self._validate(node_name,
                       node_parent)

        # Step: Create Unique Key for node
        _key = self._md5(self._key(node_name,
                                   node_parent))

        # Step: Return Node from Cache (if exists)
        if _key in self._node_lookup:
            return self._node_lookup[_key]

        # Step: Augment Properties
        _node = {
            "id": _key,
            "node": {
                "name": node_name,
                "parent": node_parent,
                "schema": self._dim_finder.find(node_parent)[0]},
            "manifest": self._add_manifest_properties(manifest),
            "provenance": self._add_provenance_properties(provenance)}

        # Step: Assign Node to Cache
        self._node_lookup[_key] = _node

        if is_debug:
            self.logger.debug("\n".join([
                "Generated Node",
                pprint.pformat(_node, indent=4)]))

        return _node

    def explain(self):
        total_nodes = len(self._node_lookup)

        self.logger.debug(f"Graph Context Insight ("
                          f"total-nodes={total_nodes})")
