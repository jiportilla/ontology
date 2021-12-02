#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from pandas import Series

from base import BaseObject


class GeneratePolarityScore(BaseObject):
    """  Generate a Polarity Score

    """

    def __init__(self,
                 df_report: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            23-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1441
        :param df_report:
            Sample Input:
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
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._df_report = df_report

    def process(self,
                log_threshold: int = 1000) -> DataFrame:

        self.logger.debug("Start Generate Polarity Score")

        master = []
        record_ids = self._df_report['RecordID'].unique()

        ctr = 0
        total_records = len(record_ids)

        for record_id in record_ids:

            ctr += 1
            # if ctr % log_threshold == 0:
            self.logger.debug(f"Status: {ctr}-{total_records}")

            df_by_record_id = self._df_report[self._df_report['RecordID'] == record_id]

            pos_tags = sorted(df_by_record_id[df_by_record_id['Category'] == 'Pros']['Tag'].unique())
            neg_tags = sorted(df_by_record_id[df_by_record_id['Category'] == 'Cons']['Tag'].unique())
            sgs_tags = sorted(df_by_record_id[df_by_record_id['Category'] == 'Suggestions']['Tag'].unique())

            pos_score = len(pos_tags)
            pos_mult = len([x for x in pos_tags if 'positive' in x.lower()])

            neg_score = len(neg_tags)
            neg_mult = len([x for x in pos_tags if 'negative' in x.lower()])

            sgs_score = len(sgs_tags)

            pos = pos_score * pos_mult
            neg = neg_score * neg_mult
            sgs = sgs_score

            def _first_row() -> Series:
                for _, row in df_by_record_id.iterrows():
                    return row

            d_row = dict(_first_row().to_dict())

            del d_row['Tag']
            del d_row['Schema']
            del d_row['Category']

            d_row['Positive'] = pos
            d_row['Negative'] = neg
            d_row['Neutral'] = sgs

            master.append(d_row)

        return pd.DataFrame(master)
