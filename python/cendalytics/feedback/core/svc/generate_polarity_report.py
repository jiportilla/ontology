#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import LabelFormatter


class GeneratePolarityReport(BaseObject):
    """  Generate a Summary of Sentiment Tags

    Sample Output:
        +----+-------------+-----------+--------------+--------------------------------------+---------------+-----------------------+----------+
        |    | Category    | Country   | Leadership   | RecordID                             | Schema        | Tag                   | Tenure   |
        |----+-------------+-----------+--------------+--------------------------------------+---------------+-----------------------+----------|
        |  0 | Suggestions | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Test          | Education             | Test     |
        |  1 | Suggestions | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Test          | Career Conversation   | Test     |
        |  2 | Suggestions | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Collaboration | Career Conversation   | Test     |
        |  3 | Suggestions | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Promotion     | Career Conversation   | Test     |
        |  4 | Pros        | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Test          | Positive Leadership   | Test     |
        |  5 | Pros        | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Test          | Supportive            | Test     |
        |  6 | Pros        | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Test          | Positive Team Culture | Test     |
        |  7 | Other       | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Test          | Request               | Test     |
        |  8 | Cons        | Test      | Test         | 8d30ec64-0e74-11ea-b45d-acde48001122 | Test          | Career Growth         | Test     |
        +----+-------------+-----------+--------------+--------------------------------------+---------------+-----------------------+----------+
    """

    __l_pros = [
        'positive team culture',
        'positive leadership',
        'supportive',
        'appreciation',
        'satisfied',
    ]

    __l_cons = [
        'career growth',
        'no conversation',
        'no career growth',
        'improve culture',
        'promotion',
        'pay raise',
        'churn',
        'employee churn',
        'no change',
        'age',
        'argument',
        'bad process',
        'bad quality',
        'behavior',
        'benefit',
        'benefit pay',
        'bias',
        'bonus pay',
        'cost cutting',
        'cost of living',
        'cost reduction',
    ]

    __l_suggestions = [
        'education',
        'career conversation',
        'appraisal',
        'checkpoint',
        'clients',
        'collaborative culture',
        'collaboration',
        'customer support',
        'customer experience'
    ]

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

    def _categorize(self,
                    tag: str) -> str or None:
        tag = tag.lower().strip()

        if tag in self.__l_pros:
            return "Pros"
        if tag in self.__l_cons:
            return "Cons"
        if tag in self.__l_suggestions:
            return "Suggestions"

    @staticmethod
    def _case(input_text: str) -> str:
        return LabelFormatter.camel_case(input_text, split_tokens=True)

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

                def _categorize():
                    category = self._categorize(row['Tag'])
                    if category:
                        return category

                    for schema in row['Schema']:
                        category = self._categorize(schema)
                        if category:
                            return category

                    return 'Other'

                d_row = SentimentRecordStructure.deep_copy(row)

                d_row['Tag'] = self._case(row['Tag'])
                d_row['Category'] = self._case(_categorize())

                master.append(d_row)

        return pd.DataFrame(master).sort_values(by=['RecordID'], ascending=False)
