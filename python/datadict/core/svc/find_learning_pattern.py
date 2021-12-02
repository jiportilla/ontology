# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO


class FindLearningPattern(BaseObject):
    """ One-Stop-Shop Service API for Learning Data Patterns """

    def __init__(self):
        """
        Created:
            24-Jul-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/475
        """
        BaseObject.__init__(self, __name__)

        lines = FileIO.file_to_lines(FileIO.absolute_path(
            "config/reporting/learning_noise.csv"))
        self.lines = [x.lower() for x in lines if x and len(x) > 3]

    def _exists(self,
                input_text: str) -> bool:
        for noise_value in self.lines:
            if input_text in noise_value:
                return True
        return False

    def remove_noise(self,
                     input_text: str) -> str:

        input_text = input_text.lower()
        for line in self.lines:
            if line in input_text:
                input_text = input_text.replace(line, "")

        return input_text.strip()
