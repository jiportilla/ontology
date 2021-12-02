# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject
from base import MandatoryParamError

PROV_WILDCARD = "entity"
DEFAULT_SEMANTIC_TYPE = "Thing"


# Lots of pylint false positives :-(
# pylint: disable=unsupported-membership-test
# pylint: disable=unsubscriptable-object
# pylint: disable=unsupported-assignment-operation

class FindEntity(BaseObject):
    """ One-Stop-Shop Service API for Entity queries """

    _l_labels = None
    _d_parents = None
    _d_patterns = None
    _d_parents_revmap = None
    _d_patterns_revmap = None
    _d_lowercase_parents = None
    _l_lowercase_labels = None

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            12-Apr-2017
            craig.trim@ibm.com
        Updated:
            28-Apr-2017
            craig.trim@ibm.com
            *   added 'scoped'
            *   updated comments
        Updated:
            3-Jul-2017
            craig.trim@ibm.com
            *   added 'find-children' and 'has-children'
            *   added 'find-parent' and 'has-parent'
        Updated:
            4-Jul-2017
            craig.trim@ibm.com
            *   added 'descendants'
        Updated:
            28-Jul-2017
            craig.trim@ibm.com
            *   integrated 'find-entity-hierarchy'
                removed custom children/descendants methods
        Updated:
            1-Aug-2017
            craig.trim@ibm.com
            *   add find-entity by prov
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            26-Mar-2019
            craig.trim@ibm.com
            *   near complete re-write
        Updated:
            1-Apr-2019
            craig.trim@ibm.com
            *   added pattern dictionary search in 'label' function
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)
        from datadict.core.dmo import DictionaryLoader

        self.is_debug = is_debug
        _loader = DictionaryLoader(is_debug=is_debug,
                                   ontology_name=ontology_name)

        self._l_labels = _loader.taxonomy().labels()
        self._d_parents = _loader.taxonomy().parents()
        self._d_patterns = _loader.taxonomy().patterns()

    def d_parents_revmap(self) -> dict:
        """
        :return:
            a reconstructed dictionary where the parent is the key
            and the children are the values
        """
        if self._d_parents_revmap is None:
            d_parents_revmap = {}
            for k in self._d_parents:
                for v in self._d_parents[k]:
                    if v not in d_parents_revmap:
                        d_parents_revmap[v] = set()
                    d_parents_revmap[v].add(k)
            self._d_parents_revmap = d_parents_revmap
        return self._d_parents_revmap

    def _lowercase_parents(self) -> dict:
        if self._d_lowercase_parents is None:
            self._d_lowercase_parents = {key.lower(): self._d_parents[key] for key in self._d_parents}
        return self._d_lowercase_parents

    def _lowercase_labels(self) -> list:
        if self._l_lowercase_labels is None:
            self._l_lowercase_labels = [x.lower() for x in self._l_labels]
        return self._l_lowercase_labels

    def _revmap_patterns(self) -> dict:
        if self._d_patterns_revmap is None:
            self._d_patterns_revmap = {}
            for key, values in self._d_patterns.items():
                for value in values:
                    if '+' in value:
                        continue
                    self._d_patterns_revmap[value] = key
        return self._d_patterns_revmap

    def all_labels(self) -> list:
        return self._l_labels

    def label_or_self(self,
                      some_input: str) -> str:
        label = self.label(some_input)
        if label:
            return label
        return some_input

    def label(self,
              some_input: str) -> Optional[str]:

        _input = some_input.lower().strip()

        _lowercase_labels = self._lowercase_labels()
        if _input in _lowercase_labels:
            return self._l_labels[_lowercase_labels.index(_input)]

        revmap_patterns = self._revmap_patterns()
        if _input in revmap_patterns:
            return revmap_patterns[_input]

        # ****************************************************
        # ****      CAUTION: DO NOT RETURN THE INPUT      ****
        # ****      This Function Must Return None        ****
        # ****      https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/171
        # ****************************************************

    def exists(self,
               some_input: str) -> bool:
        return self.label(some_input) is not None

    def parents(self,
                some_input: str) -> list:
        parents = self._lowercase_parents()
        _input = some_input.lower().strip()
        if _input in parents:
            return parents[_input]
        else:
            return []

    def first_parent(self,
                     some_input: str,
                     raise_warning=True,
                     raise_value_error=True) -> str:
        """
        :param some_input:
            the tag that the function is finding a parent for
        :param raise_warning:
            if True     (default) log a warning if multiple parents exists
                        and only the first one is returned
        :param raise_value_error:
            if True     (default) raise an exception if no parent exists
            if False    return "unknown"
        :return:
            the first parent for a tag
        """
        if not some_input:
            raise MandatoryParamError("Tag Name")

        if some_input.lower().strip() == "thing":
            return "root"

        parents = self.parents(some_input)
        if parents and len(parents):

            if raise_warning and len(parents) > 1:
                self.logger.warning("\n".join([
                    "Returning First Parent Only",
                    "\tTag Name: {}".format(some_input),
                    "\tParents: {}".format(parents)]))

            return parents[0]

        if raise_value_error:
            raise ValueError("\n".join([
                "No Parent Found",
                "\tTag Name: {}".format(some_input)]))

        return "unknown"

    def ancestors(self,
                  some_input: str) -> list:

        results = set()

        def _parents(value: str):
            if value != some_input:
                results.add(value)
            for parent in self.parents(value):
                _parents(parent)

        _parents(some_input)
        return sorted(results)

    def children(self,
                 some_input: str) -> list:
        children = set()

        _input = some_input.lower().strip()
        for key in self._d_parents:
            for value in self._d_parents[key]:
                if value.lower().strip() == _input:
                    children.add(key)

        return sorted(children)

    def descendants(self,
                    some_input: str) -> list:
        results = set()

        def _children(value: str):
            if value != some_input:
                results.add(value)
            for parent in self.children(value):
                _children(parent)

        _children(some_input)
        return sorted(results)

    def has_descendant(self,
                       some_input: str,
                       some_descendant: str) -> bool:
        _descendant = some_descendant.lower().strip()
        return _descendant in [x.lower().strip()
                               for x in self.descendants(some_input) if x]

    def has_ancestor(self,
                     some_input: str,
                     some_ancestor: str) -> bool:
        _ancestor = some_ancestor.lower().strip()
        return _ancestor in [x.lower().strip()
                             for x in self.ancestors(some_input) if x]

    def has_ancestor_from_list(self,
                               some_input: str,
                               some_ancestors: list) -> bool:
        """
        :param some_input:
        :param some_ancestors:
            a list of candidate ancestors
        :return:
            True    if at least one ancestor on the list matches
            False   if no ancestors match
        """
        for ancestor in some_ancestors:
            if self.has_ancestor(some_input=some_input,
                                 some_ancestor=ancestor):
                return True
        return False

    def has_child(self,
                  some_input: str,
                  some_child: str) -> bool:
        _child = some_child.lower().strip()
        return _child in [x.lower().strip()
                          for x in self.children(some_input) if x]

    def has_parent(self,
                   some_input: str,
                   some_parent: str) -> bool:
        _parent = some_parent.lower().strip()
        parents = [x.lower()
                   for x in self.parents(some_input)]

        return _parent in parents
