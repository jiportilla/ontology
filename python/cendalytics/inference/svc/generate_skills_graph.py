#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import sys

from base import BaseObject
from cendalytics.tfidf.core.bp import VectorSpaceAPI


class GenerateSkillsGraph(BaseObject):
    """ Inference API """

    def __init__(self,
                 serial_number: str,
                 is_debug: bool = False):
        """
        Created:
            6-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1230
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._serial_number = serial_number
        self._vectorspace_api = VectorSpaceAPI(is_debug=self._is_debug)

    def process(self):
        df_skills = self._vectorspace_api.read(library_name="SUPPLY_TAG_20190701_TFIDF.csv").process(key_field=self._serial_number,
                                                                          top_n=sys.maxsize)



def main():
    GenerateSkillsGraph(serial_number="1A5085897", is_debug=True).process()


if __name__ == "__main__":
    main()
