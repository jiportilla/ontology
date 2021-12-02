# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import CendantCollection


class DataTransformerForJrs(BaseObject):
    """ Transforms a parsed JRS record set
    """

    def __init__(self):
        """
        Created:
            5-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def _records():
        return CendantCollection(some_db_name="cendant",
                                 some_collection_name="jrs_parsed").all()

    def process(self) -> list:

        l_records = []
        blacklist = ["is_shortlist", "indirect_jrll"]
        for record in self._records():

            d_record = {}
            for field in record["fields"]:

                if field["name"] in blacklist:
                    continue

                def _tags():
                    if "tags" in field:
                        return sorted(set(field["tags"]["supervised"] + field["tags"]["unsupervised"]))

                d_record[field["name"]] = {
                    "value": field["value"],
                    "tags": _tags()
                }

            l_records.append(d_record)

        return l_records
