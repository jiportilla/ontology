#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time
from typing import Callable, Iterator, Iterable, Optional, Union

from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from base import BaseObject
from base import MandatoryParamError
from datamongo.core.dmo import BaseMongoClient
from datamongo.core.dmo import BaseMongoHelper
from datamongo.core.svc import CreateRecord
from datamongo.core.svc import DeleteRecord
from datamongo.core.svc import ReadRecord


class CendantCollection(BaseObject):
    """ generic cendant collection """

    def __init__(self,
                 some_collection_name: str,
                 some_db_name: str = 'cendant',
                 some_base_client: BaseMongoClient = None,
                 is_debug: bool = True):
        """
        Created:
            14-Apr-2019
            craig.trim@ibm.com
            *   based on 'cendant-collection-1'
        Updated:
            25-Apr-2019
            craig.trim@ibm.com
            *   make base-client optional
        Updated:
            31-May-2019
            craig.trim@ibm.com
            *   updated 'delete_records_by_key_field'
            *   updated documentation
        Updated:
            1-Nov-2019
            craig.trim@ibm.com
            *   added 'chunk' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1151#issuecomment-15694956
        Updated:
            13-Nov-2019
            craig.trim@ibm.com
            *   update 'by-field' query
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1349#issuecomment-15989359
        Updated:
            24-Nov-2019
            xavier.verges@es.ibm.com
            *   fix chuck function
            *   some type annotation to keep mypy happy
        Updated:
            20-Jab-2020
            xavier.verges@es.ibm.com
            *   by_chunks() provides a more efficient way to iterate than all()
        Updated:
            22-Feb-2020
            xavier.verges@es.ibm.com
            *   POTENTIALLY BREAKING CHANGE: by default, all() returns
                a generator instead of a list. Your regular iterating across
                records will still work, but referring to them by index won't,
                unless you do list(collection.all()) or collection.all(as_list=True).
        :param some_base_client:
            an instantiated base mongo client
        :param some_db_name:
            a mongo database name
        :param some_collection_name:
            a mongo collection name
        """
        BaseObject.__init__(self, __name__)
        if not some_db_name:
            raise MandatoryParamError("DB Name")
        if not some_collection_name:
            raise MandatoryParamError("Collection Name")
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.db_name = some_db_name
        self.collection_name = some_collection_name

        self.is_debug = is_debug
        self.base_client = some_base_client
        self.collection = some_base_client.client[some_db_name][some_collection_name]

        self.helper = BaseMongoHelper(self.collection)
        self.read_record = ReadRecord(self.collection)
        self.delete_record = DeleteRecord(self.collection)

        if self.is_debug:
            self.logger.debug('\n'.join([
                "MongoDB Connection",
                f"\tDB: {self.db_name}",
                f"\tMongo URL: {self.base_client.url}",
                f"\tCollection: {self.collection_name}"]))

    class ResultsIterator(object):
        """Iterator to get a full collection without having it all at memory at one

        collection = CendantCollection('name')
        records = collection(as_list=False)
        for record in records:
            ...
        """
        def __init__(self, collection, limit):
            self.collection = collection
            self.limit = limit

        def __len__(self):
            if self.limit:
                return self.limit
            else:
                return self.collection.count()

        def __iter__(self):
            self._gen = self._generator()
            return self     # return something with a next() method

        def __next__(self):
            return next(self._gen)

        def _generator(self):
            counter = 0
            #for chunk in self.collection.by_chunks():
            for document in self.collection.all(as_list=True):
                #for document in chunk:
                    #counter += 1
                    #if self.limit and self.limit < counter:
                    #    return
                    #else:
                yield document

    def log(self) -> str:
        return "\n\t".join([
            f"DB Name: {self.db_name}",
            f"URL: {self.base_client.url}",
            f"Collection Name: {self.collection.name}"])

    def _collection(self,
                    db_name: str,
                    collection_name: str) -> Collection:
        """
        :param db_name:
            the MongoDB db name
        :param collection_name:
            the MongoDB collection name
        :return:
            a MongoDB collection object
        """
        return self.base_client.client[db_name][collection_name]

    def save(self, some_obj, caller=None):
        return CreateRecord(self.collection).save(some_obj=some_obj,
                                                  some_caller=caller,
                                                  overwrite=True)

    def find_by_query(self,
                      some_query: dict,
                      limit: int = None) -> Optional[list]:
        """
        Generic Method to Query MongoDB
        :param some_query:
            any valid MongoDB query
        :param limit:
            a limit on the number of results
        :return:
            a list of results with cardinality of 0..*
        """
        results = self.read_record.read_queries.find_by_query(some_query=some_query,
                                                              limit=limit)
        return self.helper.results(results)

    def insert_many(self,
                    documents: list,
                    some_caller: str = None,
                    ordered_but_slower: bool = True,
                    augment_record: bool = True,
                    max_attempts: int = 1,
                    failure_logger: Callable = None) -> int:
        """
        Performs a Bulk Insert
        :param documents:
            a list of dictionaries
        :param some_caller:
            (Optional) the name of the function, class or method that is performing the insert
            typically used for purposes of activity provenance
        :param ordered_but_slower:
        :return:
            total records inserted
        """

        def _augment(some_obj):

            if "caller" not in some_obj and some_caller:
                some_obj["caller"] = some_caller

            if "ts" not in some_obj:
                some_obj["ts"] = BaseObject.generate_tts(ensure_random=False)

        if augment_record:
            [_augment(x) for x in documents]
        failures = 0
        while True:
            try:
                self.collection.insert_many(documents, ordered=ordered_but_slower)
                break
            except BulkWriteError as xcpt:
                failures += 1
                self.logger.exception(f'BulkWriteError {failures} failure of max {max_attempts}')
                if failures == 1 and failure_logger:
                    failure_logger(documents)
                if failures >= max_attempts:
                    raise xcpt

        if self.is_debug:
            self.logger.debug("\n".join([
                f"Bulk Insert: ("
                f"mongo_url={self.base_client.url}, "
                f"collection={self.collection_name}, "
                f"total={len(documents)})"]))

        return len(documents)

    def random(self,
               total_records: int = 1) -> list:
        """
        Returns a Random Record
        :param total_records:
            the number of random records to return
        :return:
            a list of random records with a cardinality of 0..*
        """
        return self.read_record.random(total_records)

    def count(self) -> int:
        """
        :return:
            a count of the number of records in this MongoDB collection
        """
        # estimated_document_count() is much faster than count_documents()
        # It is based on metada rather than on looping through all the documents.
        # It is accurate unless
        # - After an unclean shutdown
        # - On a sharded cluster, the resulting count will not correctly
        #   filter out orphaned document
        # From https://docs.mongodb.com/manual/reference/method/db.collection.count/#db.collection.count
        return self.collection.estimated_document_count()

    def chunk(self,
              chunk_size: int = 5000,
              total_records: int = None) -> list:
        start = time.time()

        if not total_records:
            total_records = self.count()
            if self.is_debug:
                self.logger.debug(f"Total Records: {total_records}")

        def chunk_retrieval():
            x = 0
            buffer = []
            while x < total_records:
                chunk_results = self.skip_and_limit(skip=x, limit=chunk_size)
                chunk_length = len(chunk_results)
                if not chunk_length:
                    return buffer
                buffer.extend(chunk_results)
                x += chunk_length
                self.logger.debug(f"Got {x} of {total_records} in {self.collection_name}")
            return buffer

        results = chunk_retrieval()
        if self.is_debug:
            self.logger.debug('\n'.join([
                "Retrieved Records",
                f"\tTotal Records: {len(results)}",
                f"time={round(time.time() - start, 2)}s)"]))

        return results

    def all(self,
            limit: int = None,
            as_list: bool = False) -> Iterable:
        """
        Returns all the records in this collection. Use by_chunks instead when possible.
        :param limit:
            the total number of records to return
        :param as_list:
            Use a list or an iterator. Use False unless you need to access records by index
        :return:
            a list of records/an iterator
        """
        if as_list:
            records = self.read_record.all(limit)
        else:
            records = CendantCollection.ResultsIterator(self, limit)   # type: ignore

        if self.is_debug:
            self.logger.debug('\n'.join([
                f"Retrieved Records (total={len(records)})",
                f"\tCollection Name: {self.collection_name}"]))
        return records

    def skip_and_limit(self,
                       skip: int,
                       limit: int) -> list:
        """
        Skip X records and return Y records
        :param skip:
            the number of records to skip
        :param limit:
            the total number of records to return
        :return:
            a list of records
        """
        return self.read_record.skip_and_limit(skip, limit)

    def by_chunks(self,
                  chunk_size: int = 500) -> Iterator[list]:
        """
        Iterator that returns collection chunks.

        Has much smaller memory demands for large collections.

        Supposedly faster implementation than skip+limit, according  to
        https://scalegrid.io/blog/fast-paging-with-mongodb/

        for chunk in collection.by_chunks(1000):
            for record in chunk:
                # do something with the record
        """
        last_id = ''
        while True:
            filter = {
                '_id': {'$gt': last_id}
            }
            cursor = self.collection.find(filter).limit(chunk_size)
            chunk = self.helper.to_result_set(cursor)
            cursor.close()
            if not len(chunk):
                break
            last_id = chunk[-1]['_id']
            yield chunk

    def delete(self,
               keep_indexes: bool = True) -> None:
        """
        Delete all Records
        """
        done = False
        try:
            if not keep_indexes:
                self.collection.drop()
                done = True
        except Exception:
            # Just in case we have no permissions
            pass

        if done:
            self.logger.warning(f"Dropped Collection {self.collection_name}")
        else:
            total_records = self.delete_record.all()
            self.logger.warning("\n".join([
                f"Flushed Collection: "
                f"(name={self.collection_name}, "
                f"total-records={total_records})"]))

    def delete_records_by_key_field(self,
                                    some_key_fields: list,
                                    is_debug: bool = True) -> None:
        """
        Delete Records by Key Field
        :param some_key_fields:
            a list of key fields
        :param is_debug:
        """

        def delete(some_id: str) -> int:
            return self.delete_record.by_query({"key_field": some_id})

        total_deleted = sum([delete(x) for x in some_key_fields])
        if not is_debug:
            return

        total_key_fields = len(some_key_fields)

        if total_deleted == 0:
            self.logger.warning("Identified Records Not Located")
            self.logger.debug(f"No Records Flushed "
                              f"(name={self.collection_name}, "
                              f"total-ids={total_key_fields})")

        elif total_deleted != total_key_fields:
            self.logger.info("Identified Records partially located")
            self.logger.debug(f"Partial Collection Flush "
                              f"(name={self.collection_name}, "
                              f"total-ids={total_key_fields}, "
                              f"total-deleted={total_deleted})")

        else:
            self.logger.info("Identified Records fully located")
            self.logger.debug(f"Flushed Collection "
                              f"(name={self.collection_name}, "
                              f"total-ids={total_key_fields}, "
                              f"total-deleted={total_deleted})")

    def by_field(self,
                 field_name: str,
                 field_value: Union[str, list]) -> Optional[list]:
        """
        Query a MongoDB Cendant Collection by Field
        :param field_name:
            some field name (a key)
        :param field_value:
            some field value (a value)
        :return:
            a single result
        """

        if type(field_value) == list and len(field_value) == 1:
            field_value = field_value[0]

        def construct_query() -> dict:
            if type(field_value) == str:
                return {field_name: field_value}
            elif type(field_value) == list:
                q = {"$or": []}  # type: ignore
                for value in field_value:
                    q["$or"].append({field_name: value})
                return q
            raise NotImplementedError

        query = construct_query()
        if self.is_debug:
            self.logger.debug('\n'.join([
                "Field Search",
                f"\tName: {field_name}",
                f"\tValue: {field_value}",
                f"\tQuery: {query}"]))

        return self.find_by_query(query)

    def distinct(self,
                 field_name: str) -> list:
        """
        Purpose:
            Query MongoDB for Distinct Values by Field Name
        :param field_name:
            some field name (a key)
        :return:
            distinct results
        """
        return self.collection.distinct(field_name)

    def by_key_field(self,
                     field_value: str = "random") -> Optional[dict]:
        """
        Query a MongoDB Cendant Collection by the Key Field (e.g., PK)
        :param field_value:
            some field value (a value)
        :return:
            a single result
        """

        if field_value.lower().strip() == "random":
            return self.random(total_records=1)[0]

        results = self.by_field("key_field", field_value)
        if not results or not len(results):  # GIT-1546-11088933
            if self.is_debug:  # GIT-1546-16472223
                self.logger.debug(f"Record Not Found ("
                                  f"key-field={field_value})")
            return None

        assert results is not None
        return results[0]
