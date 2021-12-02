#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class AntonymsDictGenerator(BaseObject):
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
        the_inference_dict = {}

        for i, row in self.df.iterrows():
            entity = row["entity"].lower().strip()

            false_positives = row["opposites"].split(",")
            false_positives = [x.lower().strip() for x in false_positives]

            if entity not in the_inference_dict:
                the_inference_dict[entity] = set()

            for a_false_positive in false_positives:

                if "+" in a_false_positive:
                    tlist = set()
                    for ttoken in a_false_positive.split("+"):
                        tlist.add(ttoken)
                    the_inference_dict[entity].add(tuple(list(tlist)))

                else:
                    the_inference_dict[entity].add(a_false_positive)

        return self.normalize(the_inference_dict)
