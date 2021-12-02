#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class RemoveSentimentTags(BaseObject):
    """  Remove Sentiment Tags

    Remove some Cendant Annotations in a manner ensures a consistent theme

    Sample:
        Input:      (Positive Team Culture, Negative Workplace)
        Action:     remove 'Positive Team Culture'
        Reason:     error on the side of negation

    Sample Output Structure:
        +------+-----------+--------------+------------+------------+--------------------+-----------+
        |      | Country   | Leadership   |   RecordID | Schema     | Tag                | Tenure    |
        |------+-----------+--------------+------------+------------+--------------------+-----------|
        | 1629 | Australia | Non-Manager  |       2271 | Industry   | finance            | 16-20 yrs |
        |  621 | India     | Non-Manager  |      31504 | Promotion  | career growth      | 6-10 yrs  |
        | 1789 | India     | Non-Manager  |       2298 | Managers   | service management | 6-10 yrs  |
        | 1713 | India     | Non-Manager  |       2282 | Colleagues | employee           | 3-5 yrs   |
        |  687 | Australia | Non-Manager  |      31517 | Salary     | employee pay       | 16-20 yrs |
        +------+-----------+--------------+------------+------------+--------------------+-----------+
    """

    __d_remove = [
        {"if_exists": ["Negative Workplace"], "remove": ["Positive Team Culture"]}
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

    def _find_removal_tags(self,
                           tags: list) -> list:

        blacklist = set()
        tags_lower = [x.lower().strip() for x in tags]

        for action in self.__d_remove:
            trigger_tags = [x.lower().strip() for x in action['if_exists']]
            trigger_tags = [x for x in trigger_tags if x in tags_lower]

            if not len(trigger_tags):
                continue

            action_tags = [x.lower().strip() for x in action['remove']]
            action_tags = [x for x in action_tags if x in tags_lower]

            if not len(action_tags):
                continue

            [blacklist.add(x) for x in action_tags]

        return sorted(blacklist)

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
            blacklist = self._find_removal_tags(tags)

            if not len(blacklist):
                for _, row in df_by_record_id.iterrows():
                    master.append(SentimentRecordStructure.deep_copy(row))
                continue

            for _, row in df_by_record_id.iterrows():
                tag = row['Tag'].lower().strip()

                if tag in blacklist:
                    if self._is_debug:
                        self.logger.debug(f"Tag Not Copied: {tag}")
                    continue

                master.append(SentimentRecordStructure.deep_copy(row))

        return pd.DataFrame(master)
