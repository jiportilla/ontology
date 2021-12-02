#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import datetime

from base import BaseObject


class DateRangeQueryHelper(BaseObject):
    """ Generate a Generic Date Range Query   """

    def __init__(self,
                 start_date: str,
                 end_date: str = None,
                 is_debug: bool = False):
        """
        Created:
            21-May-2019
            craig.trim@ibm.com
            *   refactored out of '-demand-source'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/304
        Updated:
            7-Aug-2019
            craig.trim@ibm.com
            *   renamed from 'generate-daterange-query'
        :param start_date:
            must be in format: YYYY-MM-DD
        :param end_date:
            must be in format: YYYY-MM-DD
        """
        BaseObject.__init__(self, __name__)
        if not end_date:
            end_date = self._today()

        self.start_date = start_date
        self.end_date = end_date
        self.is_debug = is_debug

    @staticmethod
    def _today():
        now = datetime.datetime.now()
        return "{}-{}-{}".format(now.year,
                                 now.month,
                                 now.day)

    def process(self) -> dict:
        """
        Purpose:
            construct a generic date-range query
        :return:
            a mongoDB query
        """
        ts_start = "{} 00:00:00".format(self.start_date)
        ts_end = "{} 00:00:00".format(self.end_date)

        return {
            '$and': [
                {
                    'fields': {
                        '$elemMatch': {
                            'name': "date.start_date",
                            'value': {
                                '$gte': ts_start
                            }
                        }
                    }
                },
                {
                    'fields': {
                        '$elemMatch': {
                            'name': "date.end_date",
                            'value': {
                                '$lt': ts_end
                            }
                        }
                    }
                }
            ]
        }
