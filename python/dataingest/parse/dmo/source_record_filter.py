#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class SourceRecordFilter(BaseObject):
    """ Remove non-Annotatable Source Records from a list """

    def __init__(self,
                 source_records: list,
                 is_debug: bool = False):
        """
        Created:
            30-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1026
        :param source_records:
            a list of records from the source collection
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._source_records = source_records

        self.logger.debug("Instantiate SourceRecordFilter")

    def process(self) -> list:
        """
        Purpose:
            Do not process records without long-text fields
        Rationale:
            1.  a record is composd of 0..* fields
            2.  each field has a type (text, long-text, badge, tc
            3.  only long-text fields are annotated
            4.  only annotated records have value value in the XDM collection
            Therefore:
            1.  if a record is not annotated, it has no value going forward
                and should be discarded
        :return:
            an updated list of source records
        """
        master = []
        for source_record in self._source_records:
            total_long_text_fields = [field for field in source_record["fields"]
                                      if field["type"] == "long-text"]
            if total_long_text_fields == 0:
                continue

            master.append(source_record)

        if len(master) != len(self._source_records):
            self.logger.info(f"Source Record Filtering Complete "
                             f"(original={len(self._source_records)}, "
                             f"filtered={len(master)})")

        return master
