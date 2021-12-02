#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import codecs
import logging
import pprint

from base import BaseObject


class EntityPatternExpansion(BaseObject):
    """
        Purpose:
    """

    def __init__(self, some_doc, debug=False):
        """
        Updated:
            26-Jul-2017
            craig.trim@ibm.com
            *   refactored out of 'entity-kb-processor'
            *   pattern expansion logic completely re-writtent to comply with 
                <https://github.ibm.com/abacus-implementation/Abacus/issues/1709>
        """
        BaseObject.__init__(self, __name__)

        if some_doc is None:
            raise ValueError

        self.doc = some_doc
        self.debug = debug

    def get_patterns_by_key(self, some_entity):
        """
        Purpose:
            get patterns by entity key
            sample input:
                "OneReset"
                sample output:
                ['one+reset', 'onereset']

        :param some_entity:
            the lookup key
        :return:
            a list of patterns
        """
        def inner(some_entity):
            if some_entity not in self.doc:
                raise ValueError("Entity Not Found: {0}".format(some_entity))

            if "patterns" in self.doc[some_entity]:
                return self.doc[some_entity]["patterns"]
            return []

        patterns = inner(some_entity)
        if not isinstance(patterns, list):
            patterns = patterns.split(",")

        # Important:
        # the name of the entity is also a pattern!
        patterns.append(some_entity)

        patterns = [x.lower().strip() for x in patterns]
        patterns = list(sorted(set(patterns)))  # de-dupe and sort

        if self.debug:
            self.logger.debug("Entity Patterns:\n\tkey = {0}\n\tpatterns = {1}".format(
                some_entity, patterns))
        return patterns

    def get_entities_by_type(self, some_type):
        """
        Purpose:
            return entities by type
            sample input:
                "product"
                sample output:
                ['OneReset', 'Onstream meeting', 'onedrive']

        :param some_type:
            some entity type 
                e.g. "product"
        :return:
            a list of all entities by type
        """
        def inner(some_type):
            matching_entities = set()
            for key in self.doc:
                if self.doc[key]["type"].upper() == some_type.upper():
                    matching_entities.add(key)
            return list(sorted(matching_entities))

        matching_entities = inner(some_type)

        if self.debug:
            self.logger.debug("Located Matching Entities\n\ttype = {0}\n\tmatches = {1}".format(
                some_type, matching_entities))
        return matching_entities

    def expand_tokens(self, some_tokens):
        """
        Purpose:        
            given a set of tokens
                [   'receive', '$product']
            expand this into a dictionary
                {   'control': 'receive', 
                    'expansions': [
                        ['one+reset', 'onereset'], 
                        ['onstream meeting'], 
                        ['onedrive']]}

                the "control" key represents the constant
                    e.g. "receive"
                the "expansions" represent the expansions by type
                    e.g. "$product"

            Implementation Notes:
                the "$" denotes an "expandable" term
                by convention, this symbol prefixes an entity type

        :param some_tokens:
            some list of strings
                each value is either an entity name (e.g. "receive")
                or an entity type (e.g. "$product")
        :return:
            a list of tokens with proper expansions
        """
        expansion_dict = {}

        def inner(some_token):
            matches = []
            lookup_token = some_token[1:]

            for entity in self.get_entities_by_type(lookup_token):
                matches.append(self.get_patterns_by_key(entity))

            return filter(lambda x: len(x) > 0, matches)

        for some_token in some_tokens:
            if not some_token.startswith("$"):
                expansion_dict["control"] = some_token
            else:
                expansion_dict["expansions"] = inner(some_token)

        if self.debug:
            self.logger.debug("Expanded Tokens:\n\toriginal = {0}\n\texpanded = {1}".format(
                some_tokens, expansion_dict))
        return expansion_dict

    def process(self):

        for key in self.doc:
            patterns = self.get_patterns_by_key(key)

            # no patterns exist in this entry
            # copy the contents into the new dictionary and move on
            if 0 == len(patterns):
                continue

            modified_patterns = set()
            for pattern in patterns:

                # looking for $alpha+beta patterns ...
                if "+" not in pattern:
                    modified_patterns.add(pattern)
                    continue

                if "$" not in pattern:
                    modified_patterns.add(pattern)
                    continue

                expansion_dict = self.expand_tokens(pattern.split("+"))
                for some_list in expansion_dict["expansions"]:
                    for pattern in some_list:
                        modified_patterns.add("{0}+{1}".format(
                            expansion_dict["control"], pattern))

            self.doc[key]["patterns"] = ",".join(modified_patterns)

        return self.doc
