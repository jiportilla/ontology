#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class EntityInferenceGenerator(BaseObject):
    def __init__(self, some_dict):
        """
        Updated:
            20-Nov-2016
            craig.trim@ibm.com
            *   added tuple support
        Updated:
            3-Aug-2017
            craig.trim@ibm.com
            *   replace pandas df with yaml doc
            *   renamed from 'inference-dict-generator'
        :param some_dict:
        """
        BaseObject.__init__(self, __name__)
        if some_dict is None:
            raise ValueError

        self.dict = some_dict

    @staticmethod
    def normalize(some_dict):
        the_normalized_dict = {}
        for key in some_dict:
            the_normalized_dict[key] = list(some_dict[key])
        return the_normalized_dict

    @staticmethod
    def cleanse(some_token):
        return some_token.lower().strip()

    def to_tuple(self, some_tokens):

        if len(some_tokens) > 2:
            raise NotImplementedError

        value = (
            self.cleanse(some_tokens[0]),
            self.cleanse(some_tokens[1]))

        return value

    def process(self):
        the_inference_dict = {}

        for key in self.dict:
            if key not in the_inference_dict:
                the_inference_dict[key] = set()

            for value in self.dict[key]:

                if "+" in value:
                    value = self.to_tuple(value.split("+"))
                the_inference_dict[key].add(value)

        return self.normalize(the_inference_dict)
