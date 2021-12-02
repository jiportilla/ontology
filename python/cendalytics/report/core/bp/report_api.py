#!/usr/bin/env python
# -*- coding: utf-8 -*-


from base import BaseObject
from cendalytics.report.core.svc import GenerateFeedbackReport
from datamongo import BaseMongoClient


class ReportAPI(BaseObject):
    """
    """

    def __init__(self,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            13-Nov-2019
            craig.trim@ibm.com
        :param mongo_client:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._mongo_client = mongo_client

    def report(self):
        class Facade(object):

            @classmethod
            def by_serial_number(cls,
                                 serial_number: str,
                                 collection_date: str = "latest") -> GenerateFeedbackReport:
                return GenerateFeedbackReport(key_field=serial_number,
                                              source_data_name="supply",
                                              collection_date=collection_date,
                                              mongo_client=self._mongo_client,
                                              is_debug=self._is_debug)

            @classmethod
            def by_openseat_id(cls,
                               openseat_id: str,
                               collection_date: str = "latest") -> GenerateFeedbackReport:
                return GenerateFeedbackReport(key_field=openseat_id,
                                              source_data_name="demand",
                                              collection_date=collection_date,
                                              mongo_client=self._mongo_client,
                                              is_debug=self._is_debug)

            @classmethod
            def by_learning_id(cls,
                               learning_id: str,
                               collection_date: str = "latest") -> GenerateFeedbackReport:
                return GenerateFeedbackReport(key_field=learning_id,
                                              source_data_name="learning",
                                              collection_date=collection_date,
                                              mongo_client=self._mongo_client,
                                              is_debug=self._is_debug)

        return Facade()
