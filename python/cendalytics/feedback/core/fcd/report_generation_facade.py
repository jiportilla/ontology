#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import time

from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class ReportGenerationFacade(BaseObject):
    """  """

    __dim_cache = {}

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            25-Nov-2019
            craig.trim@ibm.com
            *   refactored out of feedback-sentiment-orchestrator
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1441
        :param collection_name:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._collection_name = collection_name

        self.logger.debug('\n'.join([
            "Instantiate Report Generation",
            f"\tIs Debug: {is_debug}",
            f"\tCollection Name: {collection_name}"]))

    def _outpath(self,
                 file_name: str):
        tts = str(time.time()).split('.')[0].strip()
        fname = f"{self._collection_name}-{file_name}-{tts}.csv"
        return os.path.join(os.environ['GTS_BASE'], 'resources/output/analysis', fname)

    def pipeline(self,
                 df_report: DataFrame) -> DataFrame:
        """
        Purpose:
            Pipeline Orchestration
        :param df_report:
        :return:
        """

        from cendalytics.feedback.core.svc import RemoveSentimentTags
        from cendalytics.feedback.core.svc import RelabelSentimentTags
        from cendalytics.feedback.core.svc import ReplaceSentimentTags
        from cendalytics.feedback.core.svc import AugmentSentimentTags
        from cendalytics.feedback.core.svc import GeneratePolarityReport

        df_report = AugmentSentimentTags(df_report=df_report,
                                         is_debug=self._is_debug).process()
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Sentiment Tag Augmentation Complete",
                tabulate(df_report.sample(5), tablefmt='psql', headers='keys')]))

        df_report = RemoveSentimentTags(df_report=df_report,
                                        is_debug=self._is_debug).process()
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Sentiment Tag Removal Complete",
                tabulate(df_report.sample(5), tablefmt='psql', headers='keys')]))

        df_report = RelabelSentimentTags(df_report=df_report,
                                         is_debug=self._is_debug).process()
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Sentiment Tag Relabelling Complete",
                tabulate(df_report.sample(5), tablefmt='psql', headers='keys')]))

        df_report = ReplaceSentimentTags(df_report=df_report,
                                         is_debug=self._is_debug).process()
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Sentiment Tag Replacement Complete",
                tabulate(df_report.sample(5), tablefmt='psql', headers='keys')]))

        df_report = GeneratePolarityReport(df_report=df_report,
                                           is_debug=self._is_debug).process()
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Sentiment Summary Complete",
                tabulate(df_report.sample(5), tablefmt='psql', headers='keys')]))

        return df_report

    def process(self,
                total_records: int = None):
        from cendalytics.feedback.core.svc import GenerateSegmentsReport
        from cendalytics.feedback.core.svc import GeneratePolarityScore
        from cendalytics.feedback.core.svc import RetrieveSourceRecords
        from cendalytics.feedback.core.svc import GenerateMetaSentiment

        records = RetrieveSourceRecords(is_debug=self._is_debug,
                                        collection_name=self._collection_name).process(total_records=total_records)
        if not records or not len(records):
            self.logger.error('\n'.join([
                "No Records Found",
                f"\tCollection Name: {self._collection_name}"]))
            raise ValueError

        df_report = GenerateSegmentsReport(records=records,
                                           is_debug=self._is_debug).process()

        df_report = self.pipeline(df_report)
        df_report.to_csv(self._outpath("summary"), encoding="utf-8", sep="\t")

        self.logger.debug('\n'.join([
            "Generated Summary Report",
            tabulate(df_report.sample(5), tablefmt='psql', headers='keys')]))

        df_polarity = GeneratePolarityScore(df_report=df_report).process(log_threshold=10)
        df_polarity.to_csv(self._outpath("polarity"), encoding="utf-8", sep="\t")

        self.logger.debug('\n'.join([
            "Generated Polarity Report",
            tabulate(df_polarity.sample(5), tablefmt='psql', headers='keys')]))

        df_meta = GenerateMetaSentiment(df_summary=df_report).process()
        df_meta.to_csv(self._outpath("meta"), encoding="utf-8", sep="\t")

        self.logger.debug('\n'.join([
            "Generated Meta Report",
            tabulate(df_meta.sample(5), tablefmt='psql', headers='keys')]))

        return df_report
