#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import FindEntity
from datadict import FindRelationships

INVALID_PARENTS = ["domainterm", "root"]


class RelationshipExtractor(BaseObject):
    """ extract relationships from Ontology based on parsed (tagged) data
        e.g. the source data may contain this tag:
                'OpenStack'
        and this entity carries an implication for
                'RedHat'
        which is a type of
                'Linux'
        all of which in turn have a variety of implications and other requirements and relationships """

    def __init__(self,
                 ontology_name:str,
                 is_debug:bool=False):
        """
        Created:
            28-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-dim-map'
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)

        self._entity_finder = FindEntity(is_debug=is_debug,
                                        ontology_name=ontology_name)
        self._relationship_finder = FindRelationships(is_debug=is_debug,
                                                     ontology_name=ontology_name)

    def _find_relationships(self,
                            d_rels: dict) -> dict:
        """
        Notes:
        1.  Traversing the "owns" relationships is not helpful
            the subject of "owns" is typically a company and knowing what a company owns (e.g. IBM)
            doesn't provide anything of value
        :param d_rels:
        :return:
        """

        def _filter(some_list: list) -> list:
            """
            remove any tag that already exist
                (e.g.   a tag may be retrieved by multiple relationships;
                        no need to return it twice)
            :param some_list:
            :return:
                a list of (new) tags
            """
            some_list = [x.lower().strip() for x in some_list if x]
            some_list = [x for x in some_list
                         if x not in d_rels["cache"] and
                         x not in INVALID_PARENTS]
            return some_list

        for tag in d_rels["tags"]:
            d_rels["relationships"]["parents"] += _filter(self._entity_finder.parents(tag))
            d_rels["relationships"]["implies"] += _filter(self._relationship_finder.implies(tag))
            d_rels["relationships"]["requires"] += _filter(self._relationship_finder.requires(tag))
            d_rels["relationships"]["version_of"] += _filter(self._relationship_finder.version_of(tag))
            d_rels["relationships"]["has_part"] += _filter(self._relationship_finder.has_part(tag))
            d_rels["relationships"]["owned_by"] += _filter(self._relationship_finder.owned_by(tag))
            d_rels["relationships"]["part_of"] += _filter(self._relationship_finder.part_of(tag))

        headers = ["parents",
                   "implies",
                   "requires",
                   "version_of",
                   "has_part",
                   "owned_by",
                   "part_of"]
        for header in headers:
            d_rels["relationships"][header] = sorted(set(d_rels["relationships"][header]))

        return d_rels

    @staticmethod
    def _init_structure():
        return {"tags": [],
                "cache": set(),
                "relationships": {
                    "parents": [],
                    "implies": [],
                    "requires": [],
                    "version_of": [],
                    "has_part": [],
                    "owned_by": [],
                    "part_of": [],
                }}

    @staticmethod
    def _rels_to_tags(d_rels: dict):
        return sorted(set(d_rels["relationships"]["parents"] +
                          d_rels["relationships"]["implies"] +
                          d_rels["relationships"]["requires"] +
                          d_rels["relationships"]["version_of"] +
                          d_rels["relationships"]["has_part"] +
                          d_rels["relationships"]["owned_by"] +
                          d_rels["relationships"]["part_of"]))

    def initialize(self,
                   some_tags: list):
        """
        initialize a relationship graph based on a list of tags
        sample input:
            ['System Administrator', 'OpenStack']
        :param some_tags:
        :return:
            new relationship graph
        """
        d_rels = self._init_structure()
        d_rels["tags"] = sorted(set([x["name"].lower().strip() for x in some_tags]))
        d_rels = self._find_relationships(d_rels)

        return d_rels

    def update(self,
               d_rels_prior: dict):
        """
        update a relationship graph based on a prior relationship graph
        :param d_rels_prior:
        :return:
            updated relationship graph
        """
        d_rels = self._init_structure()

        def _update_cache() -> set:
            _cache = d_rels_prior["cache"]
            _tags = set(d_rels_prior["tags"])
            return _cache.union(_tags)

        d_rels["cache"] = _update_cache()
        d_rels["tags"] = self._rels_to_tags(d_rels_prior)
        d_rels = self._find_relationships(d_rels)

        return d_rels
