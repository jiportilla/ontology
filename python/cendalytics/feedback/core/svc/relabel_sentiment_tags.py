#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class RelabelSentimentTags(BaseObject):
    """  Relabel Sentiment Tags

    Re-Label some Cendant Annotations in a manner that will benefit a final report

    Examples:
        Input:                  Output:
        Internal Process        Streamline Internal Processes
        Culture                 Improve Culture
        Churn                   Employee Churn

    Sample Output Structure:
        +------+-----------+--------------+------------+--------------------+---------------------+----------+
        |      | Country   | Leadership   |   RecordID | Schema             | Tag                 | Tenure   |
        |------+-----------+--------------+------------+--------------------+---------------------+----------|
        | 1281 | India     | Non-Manager  |        550 | Workplace          | workload            | 3-5 yrs  |
        | 2198 | India     | Non-Manager  |      28525 | Colleagues         | senior management   | 1-2 yrs  |
        | 1103 | India     | Manager      |        521 | Workplace          | workload            | 6-10 yrs |
        | 2197 | India     | Non-Manager  |      28525 | Colleagues         | employee            | 1-2 yrs  |
        | 1988 | USA       | Manager      |       4036 | Positive Workplace | positive leadership | 31+ yrs  |
        +------+-----------+--------------+------------+--------------------+---------------------+----------+
    """

    __d_labeller = {  # changes to this dictionary may require changes to test-sentiment-tab-relabeller
        "internal process": "Streamline Internal Process",
        "culture": "Improve Culture",
        "churn": "Employee Churn"
    }

    def __init__(self,
                 df_report: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            23-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1441
        :param df_report:
            a DataFrame that contains text with associated tags
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._df_report = df_report

    def process(self,
                log_threshold: int = 1000) -> DataFrame:
        from cendalytics.feedback.core.dto import SentimentRecordStructure

        master = []
        record_ids = self._df_report['RecordID'].unique()

        ctr = 0
        total_records = len(record_ids)

        for record_id in record_ids:
            df_by_record_id = self._df_report[self._df_report['RecordID'] == record_id]

            ctr += 1
            if ctr % log_threshold == 0:
                self.logger.debug(f"Status: {ctr}-{total_records}")

            for _, row in df_by_record_id.iterrows():
                d_row = SentimentRecordStructure.deep_copy(row)

                tag = row['Tag'].lower().strip()
                if tag in self.__d_labeller:
                    d_row['Tag'] = self.__d_labeller[tag]

                master.append(d_row)

        return pd.DataFrame(master)
