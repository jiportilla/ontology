#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import FileIO


class PerformTopicModeling(BaseObject):

    def __init__(self):
        """
        Created:
            3-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    def by_file(self,
                some_path: str):
        from nlusvc.textacy.dmo import TextacyTopicModeler

        lines = FileIO.file_to_lines(file_path=some_path)

        results = TextacyTopicModeler(some_values=lines,
                                      number_of_topics=10,
                                      terms_per_topic=10).process()
        pprint.pprint(results)


if __name__ == "__main__":
    path = "/Users/craigtrim/Desktop/test-forma-paper.txt"
    PerformTopicModeling().by_file(path)
