#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class CendantSrc(BaseObject):
    """ Collection Wrapper over the MongoDB Collection for source collections

        The Source collection includes all original documents aggregated by key field

        Name            Key Field           Data
        supply_src      Serial Number       all employee data (CVs)
        demand_src      Open Seat ID        all open seat demand data
        learning_src    Learning ID         all learning courses        """

    _records = None

    def __init__(self,
                 collection_name: str,
                 mongo_client: BaseMongoClient = None,
                 database_name: str = "cendant",
                 is_debug: bool = False):
        """
        Created:
            16-May-2019
            craig.trim@ibm.com
            *   based on '-demand-source'
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   renamed to 'cendant-src'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/450
        Updated:
            17-Sept-2019
            craig.trim@ibm.com
            *   make collection-name mandatory
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/941#issuecomment-14691234
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._mongo_client = mongo_client
        self.collection = CendantCollection(some_base_client=mongo_client,
                                            some_db_name=database_name,
                                            some_collection_name=collection_name,
                                            is_debug=is_debug)

    def full_text_search(self,
                         some_term: str) -> dict:
        from datamongo import PerformTextQuery

        d_result, _ = PerformTextQuery(collection_name=self.collection.collection_name,
                                       mongo_client=self._mongo_client,
                                       use_normalized_text=False,
                                       div_field=[],
                                       key_field=None,
                                       is_debug=self._is_debug).process(some_term)

        return d_result

    def all(self,
            skip: int = None,
            limit: int = None):
        """
        Purpose:
            Return Multiple Records
        :param skip:
            the number of records to skip
            Optional    if None, use 0
        :param limit:
            the total number of records to return
            Optional    if None, return all
        :return:
            a list of records with a cardinality of 1..*
        """

        if self._records:
            return self._records
        if limit and not skip:
            skip = 0

        def _records():
            if not limit:
                return self.collection.all()
            return self.collection.skip_and_limit(skip, limit)

        self._records = _records()
        self.logger.debug(f"Total Records: {len(self._records)}")

        return self._records
