#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class SiblingsDictGenerator(BaseObject):
    def __init__(self, some_df):
        BaseObject.__init__(self, __name__)
        self.df = some_df

    @staticmethod
    def normalize(some_dict):
        the_normalized_dict = {}
        for key in some_dict:
            the_normalized_dict[key] = list(some_dict[key])
        return the_normalized_dict

    def process(self):
        the_siblings_dict = {}

        for i, row in self.df.iterrows():
            source = row["name"].lower().strip()
            target = [x.lower().strip() for x in row["members"].split(",")]

            if source not in the_siblings_dict:
                the_siblings_dict[source] = set()
                the_siblings_dict[source] = the_siblings_dict[source].union(
                    target)

        return self.normalize(the_siblings_dict)
