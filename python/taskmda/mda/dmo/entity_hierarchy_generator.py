#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class EntityHierarchyGenerator(BaseObject):
    def __init__(self, some_dict):
        """
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   renamed from "ProductHierarchyGenerator"        
        :param some_dict: 
        """
        BaseObject.__init__(self, __name__)
        if some_dict is None:
            raise ValueError

        self.dict = some_dict
        self.child_to_parent_dict = {}
        self.parent_children_dict = {}

    def get_child_to_parent_dict(self):
        return self.child_to_parent_dict

    def get_parent_children_dict(self):
        return self.parent_children_dict

    def normalize_parent_children_dict(self):
        the_normalized_dict = {}

        for key in self.parent_children_dict:
            the_normalized_dict[key] = list(self.parent_children_dict[key])

        return the_normalized_dict

    def process(self):

        for key in self.dict:
            self.child_to_parent_dict[key] = self.dict[key]["type"]

        for key in self.dict:

            parent = self.dict[key]["type"]
            if parent not in self.parent_children_dict:
                self.parent_children_dict[parent] = set()

            self.parent_children_dict[parent].add(key)

        self.parent_children_dict = self.normalize_parent_children_dict()
