#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject

VALUE_CONFIDENCE_THRESHOLD = 50

LIST_VALUE_TYPES = [
]

STR_VALUE_TYPES = [
    "datetime",
    "name"
]


class ValueToTagGenerator(BaseObject):
    def __init__(self, some_values, some_tags):
        """
        Created:
            9-Feb-2017
            craig.trim@ibm.com
            *   values represent output from regular expressions
                these will be created as tags (aka entities)
            *   NOTE:
                neither 'datetime' nor 'name' extraction modules appear to be working
                at one point these did work, and should be examined in "py_extract"
                (do any of our flows still require these?)
        Updated:
            14-Feb-2017
            craig.trim@ibm.com
            *   instead of doing
                    value => tag conversion
                perform
                    value => simple_value conversion
                this is per Maria's request
                see note on 903
        :param some_values:
        :param some_tags:
        """
        BaseObject.__init__(self, __name__)
        self.values = some_values
        self.tags = set(some_tags)
        self.valuemap = {}

    @staticmethod
    def filter_sizes_by_threshold(the_values):
        """
        :param the_values:
        :return: any value greater-than-or-equal-to the threshold confidence
        """
        filtered_values = []

        for a_value in the_values:
            if a_value["confidence"] >= VALUE_CONFIDENCE_THRESHOLD:
                filtered_values.append(a_value)

        return filtered_values

    def transform_value_instances(self, the_values):
        """
        Purpose:
            take a value instance and
                - update a key/value map
                - update the tag set
        Example:
            given this input:
                'sizes': [{
                    'confidence': 100,
                    'key': 'size',
                    'pattern': '(?:^|\\s)\\d{1,4}[\\s]*[PbTbGgKkMm][Bb]',
                    'value': '20GB'
                }],
            create this output:
                [ "size", "size 20gb" ]
        :param the_values:
        """
        for a_value in the_values:
            a_key = a_value["key"].lower().strip()
            a_value = a_value["value"].upper().strip()

            self.tags.add(a_key)

            if a_key == "size":
                self.tags.add(a_key + " " + a_value.lower())

            if a_key not in self.valuemap:
                self.valuemap[a_key] = set()
                self.valuemap[a_key].add(a_value)

    def populate_value_map(self):
        for a_value_type in LIST_VALUE_TYPES:
            value_instances = self.filter_sizes_by_threshold(self.values[a_value_type])
            if len(value_instances) > 0:
                self.transform_value_instances(value_instances)

    def process(self):
        self.populate_value_map()
        if 0 == len(self.valuemap.values()):
            return {}, self.tags

        return self.valuemap, self.tags
