#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import BaseMongoClient


class PersistAPI(BaseObject):
    """ API to Persist Data in DB2 """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            3-July-2019
            abhbasu3@in.ibm.com
        Updated:
            30-September-2019
            abhbasu3@in.ibm.com
            * updated `is_truncate` parameter
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1018
        Updated:
            1-Nov-2019
            craig.trim@ibm.com
            *   added push-<name>-collection methods
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1238#issue-10682083
        Updated:
            11-Nov-2019
            abhbasu3@in.ibm.com
            *   added persist collection to be executed from admin.sh
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1312
        Updated:
            23-Feb-2020
            xavier.verges@es.ibm.com
            *   Folded PushTagCollection and PushXdmCollection into PushCollection
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    @staticmethod
    def persist(input_data, schema_name, table_name):
        from dataingest.push.dmo import PersistDatatoDB
        from datadb2.core.bp import DBCredentialsAPI

        db2username, db2password = DBCredentialsAPI("WFT_USER_NAME", "WFT_PASS_WORD").process()

        PersistDatatoDB(db2username, db2password).process(input_data, schema_name, table_name, is_truncate=True)

    def push_collection(self,
                        mongo_collection_name: str,
                        transformation_type: str,
                        target_db2_table: str,
                        target_db2_schema: str = 'Cendant',
                        tag_confidence_threshold: float = None,
                        mongo_client: BaseMongoClient = None) -> None:
        """
        Purpose:
            Push a collection into DB2
        :param mongo_collection_name:
            the name of the source collection to extract the data from
        :param target_db2_table:
            the name of the target table in DB2 to write the transformed data into
        :param target_db2_schema:
            the name of the DB2 Schema to write to
        :param mongo_client:
            an instantiated mongoDB client instance
        """
        from dataingest.push.svc import PushCollection
        from dataingest.push.dmo import FieldsRecordTransformation
        from dataingest.push.dmo import TagRecordTransformation
        from dataingest.push.dmo import XdmRecordTransformation

        transformation_type = transformation_type.lower()
        transformation = {
            "fields": FieldsRecordTransformation,
            "tag": TagRecordTransformation,
            "xdm": XdmRecordTransformation
        }
        pusher = PushCollection(mongo_collection_name=mongo_collection_name,
                                transformation_class=transformation[transformation_type],
                                transformation_type=transformation_type,
                                db2_table_name=target_db2_table,
                                db2_schema_name=target_db2_schema,
                                mongo_client=mongo_client,
                                tag_confidence_threshold=tag_confidence_threshold,
                                is_debug=self._is_debug)
        pusher.process()


def main(transformation_type, data, schema, table, tag_confidence_threshold):

    def _action(tag_confidence_threshold):
        print(transformation_type)
        if transformation_type in ["tag", "xdm", "fields"]:
            PersistAPI(is_debug=True).push_collection(mongo_collection_name=data,
                                                      transformation_type=transformation_type,
                                                      target_db2_table=table,
                                                      target_db2_schema=schema,
                                                      mongo_client=BaseMongoClient(),
                                                      tag_confidence_threshold=tag_confidence_threshold)
        elif not transformation_type:
            PersistAPI(is_debug=True).persist(data, schema, table)
        else:
            raise NotImplementedError("\n".join([
                "Unknown Param: {}".format(transformation_type)]))

    if tag_confidence_threshold:
        tag_confidence_threshold = float(tag_confidence_threshold)
    else:
        tag_confidence_threshold = 0

    _action(tag_confidence_threshold)


if __name__ == "__main__":
    import plac

    plac.call(main)
