#!/usr/bin/env python

# -*- coding: utf-8 -*-

import os

import pandas as pd

from base import BaseObject
from cendalytics.report.core.dmo import FormatGenerator
from cendalytics.report.core.dmo import RankingReportWriter


class GenerateRankingReport(BaseObject):
    """
    Purpose:
        Show Supply <-> Demand Matching
    Structure:
        1.  Single Tab
        2.  The feedback demonstrates a given source dimension
                (e.g. an open-seat)
            and shows the matching target dimensions
                (e.g. the CVs)
    """

    def __init__(self,
                 some_out_dir_name: str,
                 some_source_dimension: list,
                 some_target_dimensions: list,
                 some_final_ranking: list):
        """
        Created:
            1-Apr-2019
            craig.trim@ibm.com
        :param some_out_dir_name:
            the output directory
        """
        BaseObject.__init__(self, __name__)
        if not some_out_dir_name:
            raise ValueError("Mandatory Param: Output Directory")
        if not some_source_dimension:
            raise ValueError("Mandatory Param: Source Dimension")
        if not some_target_dimensions:
            raise ValueError("Mandatory Param: Target Dimemsion")
        if not some_final_ranking:
            raise ValueError("Mandatory Param: Final Ranking")

        self.out_dir_name = some_out_dir_name

        self.some_source_dimension = some_source_dimension
        self.target_dimensions = some_target_dimensions
        self.final_ranking = some_final_ranking

        self.writer = self._writer()
        self.workbook = self.writer.book
        self.worksheet = self._worksheet()
        FormatGenerator(self.workbook,
                        "resources/config/reporting/ranking_report_format.yml").process()

    def _writer(self):
        """ generate the workbook at the defined path """
        path = os.path.join(self.out_dir_name, "feedback.xlsx")
        return pd.ExcelWriter(path)

    def _worksheet(self):
        """ generate a worksheet using an empty dataframe (pandas workaround) """
        df = pd.DataFrame(index=[], columns=[])
        df.to_excel(self.writer, 'Report', index=False)
        return self.writer.sheets['Report']

    def process(self):
        """
          Processes the logs from the input directory
          @input: Base directory containing the input and output subdirs.
          @output: None
        """

        RankingReportWriter(self.worksheet,
                            self.some_source_dimension,
                            self.target_dimensions,
                            self.final_ranking).process()

        self.writer.save()


if __name__ == "__main__":
    from datamongo import CendantCollection

    demand_cc = CendantCollection(some_collection_name='demand_xdm_20191005')
    supply_cc = CendantCollection(some_collection_name='supply_xdm_20191111')

    source = [supply_cc.by_key_field("1A5085897")]
    target = [demand_cc.by_key_field("3503272")]

    GenerateRankingReport(some_out_dir_name="/Users/craig.trimibm.com/Desktop",
                          some_source_dimension=source,
                          some_target_dimensions=target,
                          some_final_ranking=[1]).process()
