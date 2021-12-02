#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class FeedbackSentimentAPI(BaseObject):
    """  """

    __dim_cache = {}

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            23-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1441
        Updated:
            25-Nov-2019
            craig.trim@ibm.com
            *   renamed from feedback-sentiment-api
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug

    def generate(self,
                 collection_name: str):
        from cendalytics.feedback.core.fcd import ReportGenerationFacade

        class Facade(object):

            @staticmethod
            def reports() -> ReportGenerationFacade:
                return ReportGenerationFacade(is_debug=self._is_debug,
                                              collection_name=collection_name)

        return Facade
