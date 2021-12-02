#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

from base import BaseObject


class TermIndexByDocument(BaseObject):
    """
    Retrieve Source Records and Generate Document-to-Tag Dictionary

    Sample Input:
        any list of 'tag' records

    Sample Output:
        {   '000009659': {  'agile': 1, 'ibm': 1},
            '000015659': {  'design thinking': 1, 'enterprise': 1, 'role': 1},
            '000062661': {  'design thinking': 1, 'enterprise': 1, 'role': 1},
            '000072806': {  'cognitive skill': 1},
            '000076661': {  'ibm': 1, 'mentor': 1},
            '00009Z744': {  'conversation': 1,
                            'design thinking': 1,
                            'enterprise': 1,
                            'executive': 1,
                            'financial': 1,
                            'role': 1,
                            'sales skill': 2},
            '0000MF744': {  'agile': 1,
                            'ibm': 1,
                            'ibm employee': 1,
                            'internet of things': 1},
            '0000QG744': {  'agile': 1,
                            'design thinking': 1,
                            'enterprise': 1,
                            'ibm': 1,
                            'offering manager': 1,
                            'role': 1,
                            'software as a service': 1},
                            ...
            }
    """

    def __init__(self,
                 source_records: list,
                 tag_confidence_threshold: int = 65,
                 is_debug: bool = False):
        """
        Created:
            5-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-vector-space'
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_terms_in_doc = {}
        self._source_records = source_records
        self._tag_confidence_threshold = tag_confidence_threshold

    def _update_terms_in_doc(self,
                             record: dict):
        key_field = record['key_field']
        if key_field in self._d_terms_in_doc:
            self.logger.warning(f"Duplicate Record (key-field={key_field})")
        else:
            self._d_terms_in_doc[key_field] = Counter()

        for field in record["fields"]:
            if "tags" in field:
                if "supervised" in field["tags"] and field["tags"]["supervised"] is not None:
                    for tag in field["tags"]["supervised"]:
                        if tag[1] > self._tag_confidence_threshold:
                            self._d_terms_in_doc[key_field].update({tag[0]: 1})

    def _transform(self) -> dict:
        m = {}
        for k in self._d_terms_in_doc:
            d_inner = dict(self._d_terms_in_doc[k])
            if len(d_inner):
                m[k] = d_inner
        return m

    def process(self) -> dict:
        for record in self._source_records:
            self._update_terms_in_doc(record)

        d_records = self._transform()
        self.logger.debug(f"Indexed Source Records: "
                          f"(total={len(d_records)})")

        return d_records
