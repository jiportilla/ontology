#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO
from base import MandatoryParamError

from nludialog.core.dmo import DialogTextGenerator


class AnalyzeResponse(BaseObject):
    _dialog_text_generator = DialogTextGenerator()

    def __init__(self,
                 d_flow: dict):
        BaseObject.__init__(self, __name__)
        self._d_flow = d_flow

    def next(self):
        return self._d_flow['next']

    def nlg(self) -> str:
        responses = self._dialog_text_generator.nlg(self._d_flow['nlg'])
        return ". ".join(responses)


class PerformResponse(BaseObject):
    """ Respond to a Chat Event """

    def __init__(self):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   references:
                https://pypi.org/project/slackclient/
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        self._mapping = self._load_mapping()

    @staticmethod
    def _load_mapping() -> dict:
        _d_master = {}
        relative_path = "resources/dialog/flows/"
        for file in FileIO.load_files(relative_path, "yml"):
            d_file = FileIO.file_to_yaml_by_relative_path(file)
            _d_master = {**d_file, **_d_master}

        return _d_master

    def process(self,
                incoming_flow: str) -> AnalyzeResponse:
        if not incoming_flow:
            raise MandatoryParamError("Incoming Flow")

        if incoming_flow not in self._mapping:
            raise NotImplementedError("\n".join([
                "Incoming Flow Not Found (name={})".format(
                    incoming_flow)]))

        return AnalyzeResponse(self._mapping[incoming_flow])


if __name__ == "__main__":
    dialog = PerformResponse()
    print(dialog.process("CHITCHAT_GREETING").next())
    print(dialog.process("CHITCHAT_GREETING").nlg())
