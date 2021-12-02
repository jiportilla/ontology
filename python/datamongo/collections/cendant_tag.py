#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import RecordUnavailableRecord
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class CendantTag(BaseObject):
    """ Collection Wrapper over MongoDB Parse Collection
        *   supply-tag
        *   demand-tag
        *   learning-tag    """

    _records = None

    def __init__(self,
                 collection_name: str,
                 mongo_client: BaseMongoClient = None,
                 database_name: str = "cendant",
                 is_debug: bool = True):
        """
        Created:
            2-May-2019
            craig.trim@ibm.com
            *   design pattern based on '-dimensions'
        Updated:
            15-May-2019
            craig.trim@ibm.com
            *   replace 'source-name' param with 'collection-category'
        Updated:
            16-Jul-2019
            craig.trim@ibm.com
            *   renamed to 'cendant-tag'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/450
        Updated:
            26-Sept-2019
            craig.trim@ibm.com
            *   add function to query by job-role-id
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1008
        Updated:
            28-Oct-2019
            craig.trim@ibm.com
            *   add 'tags' method in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1202
        Updated:
            31-Oct-2019
            craig.trim@ibm.com
            *   add 'dataframe' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1227#issuecomment-15665110
        Updated:
            16-Jan-2020
            craig.trim@ibm.com
            *   need a capability to cache certain views to file
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17199468
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self.is_debug = is_debug
        self.mongo_client = mongo_client
        self.collection = CendantCollection(some_base_client=mongo_client,
                                            some_db_name=database_name,
                                            some_collection_name=collection_name,
                                            is_debug=self.is_debug)

    def random(self,
               total_records: int = 1) -> list:
        """
        Returns a Random Record
        :param total_records:
            the number of random records to return
        :return:
            a list of random records with a cardinality of 0..*
        """
        return self.collection.random(total_records)

    def by_field_id(self,
                    field_id: str) -> dict or None:
        """
        Purpose:
            Return a Specific Field from a Record
        :param field_id:
            any field ID
        :return:
            a field dictionary
        """
        matches = []
        for record in self.collection.by_field("fields.field_id", field_id):
            for field in record["fields"]:
                if "field_id" in field and field["field_id"] == field_id:
                    matches.append(field)

        if not len(matches):
            self.logger.warning('\n'.join([
                "Field ID Not Found",
                f"\tField ID: {field_id}",
                f"\tCollection: {self.collection}"]))
            return None

        if len(matches) > 1:
            self.logger.error('\n'.join([
                "Multiple Matches Found",
                f"\tField ID: {field_id}",
                f"\tCollection: {self.collection}",
                pprint.pformat(matches, indent=4)]))

        return matches[0]

    def by_job_role_id(self,
                       job_role_id: str,
                       limit: int = None) -> list:
        """
        Purpose:
            Query Records by Job Role ID (from the JRS Taxonomy)
        :param job_role_id:
            a Job Role ID (e.g., '042058')
        :param limit:
            Optional    limit the number of records returned
        :return:
            a list of matching records
        """
        query = {"fields.name": "job_role_id",
                 "fields.value": job_role_id}
        return self.collection.find_by_query(query, limit=limit)

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

        if self.is_debug:
            self.logger.debug(f"Total Records: "
                              f"{len(self._records)}")

        return self._records

    def tags(self,
             cnum: str):
        """
        Purpose:
            Find a Sorted List of Tags for a Record
        :return:
            a sorted list of tags
        """
        record = self.collection.by_key_field(field_value=cnum)
        if not record:
            raise RecordUnavailableRecord(cnum)

        tags = []
        for field in record["fields"]:
            if "tags" in field:
                if "supervised" in field["tags"]:
                    [tags.append(x) for x in field["tags"]["supervised"]]

        return tags
