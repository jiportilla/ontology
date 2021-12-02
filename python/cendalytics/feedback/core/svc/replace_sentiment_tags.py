#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from pandas import Series

from base import BaseObject
from base import LabelFormatter
from datadict import FindDimensions


class ReplaceSentimentTags(BaseObject):
    """  Replace Sentiment Tags

    Replace clusters of Cendant Annotations in a manner ensures a consistent theme

    Sample:
        Input:      (Speak, Company)
        Action:     replace 'Career Conversation'

    Sample Output Structure:
        +------+-----------+--------------+------------+-----------------+-----------+------------+
        |      | Country   | Leadership   |   RecordID | Schema          | Tag       | Tenure     |
        |------+-----------+--------------+------------+-----------------+-----------+------------|
        |   22 | Brazil    | Non-Manager  |       3257 | other           | program   | 11-15 yrs  |
        | 2324 | India     | Non-Manager  |      28525 | Promotion       | promotion | 1-2 yrs    |
        |  418 | India     | Non-Manager  |        252 | Emotional Skill | positive  | 6 mos-1 yr |
        | 1321 | India     | Non-Manager  |        546 | Workplace       | ibm       | 3-5 yrs    |
        | 2119 | USA       | Manager      |       4036 | Workplace       | support   | 31+ yrs    |
        +------+-----------+--------------+------------+-----------------+-----------+------------+
    """

    __d_remove = [
        {"if_exists": ["Speak", "Company"], "replace": ["Career Conversation"]},
        {"if_exists": ["Train", "Person"], "replace": ["Employee Training"]},
        {"if_exists": ["Team", "Proud"], "replace": ["Positive Team Culture"]}
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
        self._dim_finder = FindDimensions.sentiment(is_debug=False)

    def _find_replace_tags(self,
                           tags: list) -> dict:

        tags_to_add = set()
        tags_to_delete = set()

        tags_lower = [x.lower().strip() for x in tags]

        for action in self.__d_remove:
            trigger_tags = [x.lower().strip() for x in action['if_exists']]
            trigger_tags = [x for x in trigger_tags if x in tags_lower]

            if not len(trigger_tags):
                continue

            [tags_to_delete.add(x.lower().strip()) for x in trigger_tags]
            [tags_to_add.add(x.lower().strip()) for x in action['replace']]

        return {
            "add": sorted(tags_to_add),
            "delete": sorted(tags_to_delete)}

    @staticmethod
    def _row(df: DataFrame) -> Series:
        for _, row in df.iterrows():
            return row

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

            tags = sorted(df_by_record_id['Tag'].unique())
            svcresult = self._find_replace_tags(tags)

            tags_to_add = svcresult["add"]
            tags_to_delete = svcresult["delete"]

            for _, row in df_by_record_id.iterrows():
                if row['Tag'].lower() in tags_to_delete:
                    continue

                master.append(SentimentRecordStructure.deep_copy(row))

            for tag in tags_to_add:
                for xdm in self._dim_finder.find(tag):
                    row = SentimentRecordStructure.deep_copy(self._row(df_by_record_id))
                    row['Tag'] = LabelFormatter.camel_case(tag, split_tokens=True)
                    row['Schema'] = xdm
                    master.append(row)

        return pd.DataFrame(master)
