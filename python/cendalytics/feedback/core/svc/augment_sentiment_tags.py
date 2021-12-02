#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from pandas import Series

from base import BaseObject
from base import LabelFormatter
from cendalytics.feedback.core.dmo import EntityConceptResolution
from datadict import FindDimensions
from datadict import FindRelationships


class AugmentSentimentTags(BaseObject):
    """  Augment Sentiment Tags

    Augment clusters of Cendant Annotations bnased on existing tags

    Sample:
        Input:      (Cost Cutting, Train)
        Action:     augment 'Training Budget'
        Output:      (Cost Cutting, Train, Training Budget)
    """

    # NOTES - this structure can be MUCH MORE efficient
    # but this is conceptually a one-off process that I need to finish today
    # craig.trim@ibm.com 26-Nov-2019
    __d_augment = [
        {"if_exists": ["Cost", "Training"], "augment": ["Training Budget"]},
        {"if_exists": ["Cost Cutting", "Training"], "augment": ["Training Budget"]},
        {"if_exists": ["Cost Reduction", "Training"], "augment": ["Training Budget"]},
        {"if_exists": ["Cost of Living", "Training"], "augment": ["Training Budget"]},
        {"if_exists": ["Financial Plan", "Training"], "augment": ["Training Budget"]}]

    def __init__(self,
                 df_report: DataFrame,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            26-Nov-2019
            craig.trim@ibm.com
            *   this is based on replace-sentiment-tags
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1456#issuecomment-16248349
        :param df_report:
            a DataFrame that contains text with associated tags
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._df_report = df_report
        self._dim_finder = FindDimensions.sentiment(is_debug=False)
        self._rel_finder = FindRelationships(is_debug=self._is_debug,
                                             ontology_name=ontology_name)

    def _find_augmenting_tags(self,
                              tags: list) -> list:

        tags_to_add = set()

        tags_lower = [x.lower().strip() for x in tags]

        for action in self.__d_augment:
            trigger_tags = [x.lower().strip() for x in action['if_exists']]
            trigger_tags = [x for x in trigger_tags if x in tags_lower]

            if not len(trigger_tags):
                continue

            [tags_to_add.add(x.lower().strip()) for x in action['augment']]

        return sorted(tags_to_add)

    @staticmethod
    def _row(df: DataFrame) -> Series:
        for _, row in df.iterrows():
            return row

    def _tags(self,
              df_by_record_id: DataFrame) -> list:
        """
        Purpose:
            reduce every conceivable variation of 'training' or 'learning' to 'training'
        :param df_by_record_id:
        :return:
        """
        tags = set(EntityConceptResolution(tags=sorted(df_by_record_id['Tag'].unique()),
                                           is_debug=self._is_debug).process())

        s = set()
        for tag in tags:

            if tag == "Learning":
                s.add("Training")
                continue

            for parent in self._rel_finder.parents(tag):
                if parent == "Learning" or parent == "Training":
                    s.add("Learning")

            s.add(tag)

        return sorted(s)

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

            tags = self._tags(df_by_record_id)
            tags_to_add = self._find_augmenting_tags(tags)

            for _, row in df_by_record_id.iterrows():
                master.append(SentimentRecordStructure.deep_copy(row))

            for tag in tags_to_add:
                for xdm in self._dim_finder.find(tag):
                    row = SentimentRecordStructure.deep_copy(self._row(df_by_record_id))
                    row['Tag'] = LabelFormatter.camel_case(tag, split_tokens=True)
                    row['Schema'] = xdm
                    master.append(row)

        return pd.DataFrame(master)
