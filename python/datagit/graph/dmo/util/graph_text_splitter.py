# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from string import ascii_uppercase

from base import BaseObject


class GraphTextSplitter(BaseObject):
    """ Split text into lines for Graphviz Node readability """

    @staticmethod
    def split_text(input_text: str,
                   threshold: int = 20) -> str:
        splitter = GraphTextSplitter(is_debug=False,
                                     input_text=input_text)
        return splitter.process(threshold=threshold)

    @staticmethod
    def split_by_camel_case(input_text: str,
                            threshold: int = 20):
        return GraphTextSplitter.split_camel_case(input_text=input_text,
                                                  threshold=threshold)

    def __init__(self,
                 input_text: str,
                 is_debug: bool = True):
        """
        Created:
            19-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1629
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   renamed from 'node-text-splitter'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877277
        :param input_text:
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

        input_text = input_text.replace('_', ' ')
        input_text = input_text.replace('-', ' ')
        self._input_text = input_text

    @staticmethod
    def split_camel_case(input_text: str,
                         threshold: int = 20,
                         is_debug: bool = False):
        def insert_spaces() -> str:
            """
            Purpose:
                Transform a CamelCase into tokenizable-text
            Sample Input:
                'FileImportNodeBuilder'
            Sample Output:
                'File Import Node Builder'
            :return:
                a tokenizable string
            """
            buffer = []

            total = len(input_text)
            for i in range(0, total):
                ch = input_text[i]

                is_curr_uppercase = len(buffer) and ch in ascii_uppercase
                is_prior_uppercase = (i - 1 >= 0) and (input_text[i - 1] in ascii_uppercase)

                if is_curr_uppercase and not is_prior_uppercase:
                    buffer.append(' ')

                buffer.append(ch)
            return ''.join(buffer).strip()

        splitter = GraphTextSplitter(is_debug=is_debug,
                                     input_text=insert_spaces())

        return splitter.process(split_on=' ',
                                threshold=threshold)

    def process(self,
                split_on: str = ' ',
                delimter: str = '\\n',
                threshold: int = 20) -> str:

        lines = []
        tokens = self._input_text.split(split_on)

        buffer = []
        for token in tokens:
            buffer.append(token)

            if sum([len(x) for x in buffer]) >= threshold:
                lines.append(' '.join(buffer))
                buffer = []

        if len(buffer):
            lines.append(' '.join(buffer))

        result = delimter.join(lines).strip()
        result = result.replace('\n', '').strip()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Text Splitting Complete",
                f"\tOriginal: {self._input_text}",
                f"\tNormalized: {result}"]))

        return result
