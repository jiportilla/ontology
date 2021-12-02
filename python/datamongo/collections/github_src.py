#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo.core.bp import CendantCollection


class GitHubSrc(BaseObject):
    """ GitHub Src Collection Wrapper
    """

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            7-Dec-2019
            craig.trim@ibm.com
            *   renamed from 'github-data-loader'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._collection = CendantCollection(is_debug=self._is_debug,
                                             some_collection_name=collection_name)

    def flush(self):
        self._collection.delete()

    def create_indices(self):
        from datamongo.text.svc import CreateFieldIndex
        field_index_creator = CreateFieldIndex(is_debug=self._is_debug,
                                               collection=self._collection.collection)
        field_index_creator.process(field_name="key_field")
        field_index_creator.process(field_name="key_field_parent")
        field_index_creator.process(field_name="div_field")

    def insert(self,
               source_records: list) -> None:

        if not len(source_records):
            self.logger.warning("No Records Provided")
            return

        if len(source_records):
            self._collection.insert_many(documents=source_records,
                                         some_caller=str(__name__),
                                         ordered_but_slower=True)

        if self._is_debug and len(source_records):
            self.logger.debug('\n'.join([
                "GitHub Loading Completed",
                f"\tTotal Records: {len(source_records)}",
                f"\tCollection Name: {self._collection.collection_name}"]))
