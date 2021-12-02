#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

from base import BaseObject
from nlutext import TextParser


class LongTextFieldParser(BaseObject):
    """  Parse a field classified as 'long-text'
    """

    # __text_parser = TextParser()

    def __init__(self,
                 len_threshold: int = 5,
                 is_debug: bool = False):
        """
        Created:
            16-Aug-2019
            craig.trim@ibm.com
            *   refactored out of 'parse-records-from-mongo' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/768
        Updated:
            10-Sept-2019
            craig.trim@ibm.com
            *   tag iteration defect fix
                https://github.ibm.com/-cdo/unstructured-analytics/issues/894#issuecomment-14536883
        :param len_threshold:
            the minimum length of a text field for parsing to be activated
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._len_threshold = len_threshold

        if self._is_debug:
            self.logger.debug("Instantiate LongTextFieldParser")

    def _is_valid(self,
                  a_sentence: str) -> bool:
        """
        :param a_sentence:
            an input sentence of any type or length
        :return:
            True    if input sentence is non-null
                    and meets minimum length requirements
        """
        if not a_sentence:
            return False
        if len(a_sentence) < self._len_threshold:
            return False
        return True

    def _parse_sentence(self,
                        a_sentence: str) -> dict:
        """
        :param a_sentence:
            an unstructured input sentence
        :return:
            a parser service result
            of primary interest are the tags annotated (parsed) in the sentence
        """
        _start = time.time()

        text_parser = TextParser(is_debug=self._is_debug)
        _svcresult = text_parser.process(a_sentence,
                                         use_profiler=self._is_debug)
        if self._is_debug:
            parse_time = time.time() - _start
            self.logger.debug(f"Text Parser: {parse_time}s")

        return _svcresult

    def process(self,
                parse_field: dict):
        """
        :param parse_field:
        :return:
            an augmented parse field
        """
        normalized = []
        supervised = set()
        unsupervised = set()

        # Step: Filter out small sentences
        sentences = [sentence for sentence in parse_field["value"]
                     if self._is_valid(sentence)]

        # Step: Parse each sentence in the record
        for sentence in sentences:

            svcresult = self._parse_sentence(sentence)
            if not svcresult:
                continue

            def tags_by_type(tag_type: str) -> set:
                if "tags" in svcresult and tag_type in svcresult["tags"]:
                    if svcresult["tags"][tag_type]:
                        return set(svcresult["tags"][tag_type])
                return set()

            # Step: Merge results by type
            supervised = supervised.union(tags_by_type("supervised"))
            unsupervised = unsupervised.union(tags_by_type("unsupervised"))
            normalized.append(svcresult["ups"]["normalized"])

        def _tags():
            return {
                "supervised": sorted(supervised),
                "unsupervised": sorted(unsupervised)}

        parse_field["tags"] = _tags()
        parse_field["normalized"] = normalized

        return parse_field
