#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class PushCollection(BaseObject):
    """
    """

    def __init__(self,
                 mongo_collection_name: str,
                 transformation_class,
                 transformation_type: str,
                 db2_table_name: str = "",
                 db2_schema_name: str = "CENDANT",
                 mongo_client: BaseMongoClient = None,
                 tag_confidence_threshold: float = None,
                 is_debug: bool = False):
        """
        Created:
            1-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1238#issue-10682083
        Updated:
            7-November-2019
            abhbasu3@in.ibm.com
            *   insert xdm mongo collection to DB2
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1239
        Updated:
            17-Feb-2020
            abhbasu3@in.ibm.com
            *   fetch all records from mongo collection
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1858
        Updated:
            23-Feb-2020
            xavier.verges@es.ibm.com
            *   Folded PushTagCollection and PushXdmCollection into PushCollection
            *   Process the records incrementally
        :param mongo_collection_name:
            the name of the source collection to extract the data from
        :param mongo_database_name:
            the name of the mongoDB database to connect to
        :param mongo_client:
            an instantiated mongoDB client instance
        :param db2_table_name:
            the name of the target table in DB2 to write the transformed data into
        :param db2_schema_name:
            the name of the DB2 Schema to write to
        """
        BaseObject.__init__(self, __name__)
        if not mongo_collection_name:
            raise MandatoryParamError("Collection Name")

        self._is_debug = is_debug
        self._collection_name = mongo_collection_name
        self._schema_name = db2_schema_name.upper()
        self._table_name = db2_table_name.upper() if db2_table_name else mongo_collection_name.upper()
        self._collection = CendantCollection(some_collection_name=mongo_collection_name,
                                             some_base_client=mongo_client,
                                             is_debug=is_debug)
        self._transformation_class = transformation_class
        self._transformation_type = transformation_type
        self._tag_confidence_threshold = tag_confidence_threshold

    def process(self,
                skip: int = None,
                limit: int = None):
        """
        :param skip:
            the number of records to skip
            Optional    if None, use 0
        :param limit:
            the total number of records to return
            Optional    if None, return all
        :return:
        """
        from dataingest.push.dmo import PersistDatatoDB
        from datadb2.core.bp import DBCredentialsAPI

        db2username, db2password = DBCredentialsAPI("WFT_USER_NAME", "WFT_PASS_WORD").process()
        with PersistDatatoDB(db2username, db2password) as db2_writer:

            try:
                db2_writer.create_table(self._schema_name, self._table_name, self._transformation_type)
            except:  # noqa: E722
                self.logger.info("Failed to create table")
                db2_writer.clear_table(self._schema_name, self._table_name)

            chunk_size = 1000
            chunk_count = 0
            total = self._collection.count()
            for chunk in self._collection.by_chunks(chunk_size=chunk_size):
                first = chunk_count * chunk_size
                chunk_count += 1
                last = chunk_count * chunk_size - 1
                self.logger.debug(f"Pushing records {first}-{last} of {total} ({self._collection_name}->{self._table_name})")
                record_transformer = self._transformation_class(records=chunk,
                                                                is_debug=self._is_debug and chunk_count == 1,
                                                                collection_name=self._collection_name)
                df_collection = record_transformer.process(self._tag_confidence_threshold)
                df_collection.columns = [x.upper() for x in df_collection.columns]

                db2_writer.insert_dataframe_into_table(df_collection, self._schema_name, self._table_name)

        self.logger.info(f"Completed pushing {self._collection_name} to DB2")
