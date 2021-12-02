#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import DataTypeError
from datadict import LoadStopWords


class RemoveStopWords(BaseObject):
    """ Remove Stop Words from Unstructured Text """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            28-Jun-2019
            craig.trim@ibm.com
            *   refactored out of text-api
        Updated:
            9-Aug-2019
            craig.trim@ibm.com
            *   minor updates in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/688
        Updated:
            25-Feb-2020
            craig.trim@ibm.com
            *   rewriting to fix defects in this service
                (I don't understand why the prior version was so complex)
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1878
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self.loader = LoadStopWords(is_debug=self.is_debug)

    def _replace(self,
                 some_stopword: str,
                 some_input_text: str) -> str:

        original_input = some_input_text
        some_stopword = some_stopword.lower().strip()
        some_input_text = some_input_text.lower().strip()

        if some_stopword == some_input_text:
            return ""

        def substr(x: int) -> str:
            y = len(some_stopword) + x + 1
            modified_input = f"{original_input[:x]} {original_input[y:]}"
            modified_input = modified_input.replace('  ', ' ')
            return self._replace(some_stopword, modified_input)

        fq = " {} ".format(some_stopword)
        if fq in some_input_text:
            return substr(some_input_text.index(fq))

        lq = " {}".format(some_stopword)
        if some_input_text.endswith(lq):
            return substr(some_input_text.index(lq))

        rq = "{} ".format(some_stopword)
        if some_input_text.startswith(rq):
            return substr(some_input_text.index(rq))

        return some_input_text

    def process(self,
                input_text: str or list,
                aggressive: bool = False) -> str or list:
        """
        :param input_text:
        :param aggressive:
        :return:
        """

        def _stopwords():
            if not aggressive:
                return self.loader.standard()
            return self.loader.load()

        if type(input_text) == str:
            for stopword in _stopwords():
                input_text = self._replace(stopword, input_text)
            return input_text


        elif type(input_text) == list:
            results = []
            for input_item in list(input_text):
                for stopword in _stopwords():
                    input_item = self._replace(stopword, input_item)
                results.append(input_item)
            return results

        else:
            raise DataTypeError("Expected Str or List Input")
