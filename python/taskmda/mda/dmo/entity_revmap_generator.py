#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class EntityRevmapGenerator(BaseObject):
    def __init__(self, some_dict):
        """
        Updated:
            8-Dec-2016
            craig.trim@ibm.com
            *   change from key:value to key:list
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   renamed from "ProductEntityRevmapGeneratorevmapGenerator"
        Updated:
            2-Aug-2017
            craig.trim@ibm.com
            *   clean up params/init section
            *   iterate dictionary by prov:
                <https://github.ibm.com/abacus-implementation/Abacus/issues/1721#issuecomment-3080923>
        """
        BaseObject.__init__(self, __name__)

        if some_dict is None:
            raise ValueError

        self.dict = some_dict

    @staticmethod
    def normalize(some_revmap):
        the_normalized_map = {}

        for key in some_revmap:
            the_normalized_map[key] = list(some_revmap[key])

        return the_normalized_map

    def create(self):
        d = {}

        for key in self.dict:
            for variant in self.dict[key]["variations"]:
                if variant not in d:
                    d[variant] = set()
                d[variant].add(key)

        return self.normalize(d)

    def process(self):
        return self.create()
