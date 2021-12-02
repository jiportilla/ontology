#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import copy
import pprint

from base import BaseObject
from nlutext import PerformSentenceSegmentation


class ParseSentenceSegmentizer(BaseObject):
    """ split fields by sentences

        sample input:
            record
                field
                    name:   summary
                    type:   long-text
                    value:  this is a long sentence.
                            here is the second part of the sentence.
                            here is the final sentence; also very long

        sample output:
            record
                field
                    name:   summary
                    type:   long-text
                    value:  this is a long sentence.
                field
                    name:   summary
                    type:   long-text
                    value:  here is the second part of the sentence.
                field
                    name:   summary
                    type:   long-text
                    value:  here is the final sentence; also very long

    Rationale:
        helps with annotation improvements.

        some fields have paragraph level amounts of text,
        and this makes it challenging to determine which part of the text the tag corresponds to
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            4-Apr-2019
            craig.trim@ibm.com
            *   refactored out of 'parse-manifest-data'
        Updated:
            16-Apr-2019
            craig.trim@ibm.com
            *   use deep-copy to preserve field attributes
        Updated:
            26-Aug-2019
            craig.trim@ibm.com
            *   preserve UUID integrity
                https://github.ibm.com/GTS-CDO/unstructured-analytics/pull/837
        Updated:
            15-Nov-2019
            craig.trim@ibm.com
            *   updates in support of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1378
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = True
        self._sentencizer = PerformSentenceSegmentation(is_debug=is_debug)

        if is_debug:
            self.logger.debug("Instantiate ParseSentenceSegmentizer")

    def process(self,
                record: dict) -> dict:
        """ segment into sentences if appropriate """

        _fields = []

        total_original_fields = len(record["fields"])

        for field in record["fields"]:

            if not field["value"]:
                continue

            if field["type"] != "long-text":
                field["value"] = [field["value"]]
                _fields.append(field)
                continue

            if not field["value"]:
                raise ValueError("\n".join([
                    "Invalid Field",
                    pprint.pformat(field, indent=4)]))

            ctr = 0
            for sentence in self._sentencizer.process(field["value"]):
                if sentence:
                    sentence = sentence.replace('•', '').strip()
                    if not len(sentence):
                        continue
                _field = copy.deepcopy(field)
                _field["value"] = [sentence]
                _field["field_id"] = f"{field['field_id']}-{ctr}"
                _fields.append(_field)
                ctr += 1

        record["fields"] = _fields

        total_modified_fields = len(_fields)
        if self._is_debug and total_original_fields != total_modified_fields:
            self.logger.debug(f"Sentence Segmentation Complete "
                              f"(original={total_original_fields}, "
                              f"modified={total_modified_fields})")

        return record
