#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from random import randint

from base import BaseObject
from base import FileIO


class DialogTextGenerator(BaseObject):
    """ Generate a Dialog Response """

    def __init__(self):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        self._responses = self._load_responses()

    @staticmethod
    def _load_responses() -> dict:
        _d_master = {}
        relative_path = "resources/dialog/text/"
        for file in FileIO.load_files(relative_path, "yml"):
            d_file = FileIO.file_to_yaml_by_relative_path(file)
            _d_master = {**d_file, **_d_master}

        return _d_master

    def nlg(self,
            response_names: list) -> list:

        responses = []
        for response in response_names:

            if response not in self._responses:
                raise NotImplementedError("\n".join([
                    "Response Not Found (name={})".format(
                        response)]))

            candidates = self._responses[response]["text"]

            rand = randint(0, len(candidates) - 1)
            responses.append(candidates[rand])

        return responses
