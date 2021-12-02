#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import time

from pandas.io.excel import ExcelWriter

from base import BaseObject
from cendalytics.report.core.dmo import AcademicDimensionWriter
from cendalytics.report.core.dmo import DimensionTagWriter
from cendalytics.report.core.dmo import DimensionWeightWriter
from cendalytics.report.core.dmo import ExcelWriterInitializer
from cendalytics.report.core.dmo import RecordRetriever
from cendalytics.report.core.dmo import TagInferenceWriter
from cendalytics.report.core.dmo import TagReportWriter
from cendalytics.report.core.dmo import TimeDimensionWriter
from datamongo import BaseMongoClient
from datamongo import CendantCollectionRegistry


class GenerateFeedbackReport(BaseObject):
    """
    Purpose:
        For a given record (Serial Number or Open Seat ID)
        show all the tags that were parsed out
        and line these tags up by data source (MongoDB collection) and actual sentence
    Structure:
        1.  Single Tab
        2.  The columns show the dimensions
        3.  The rows show the data sources on the left
        4.  Each entry by row/column is a tag from that data source for that dimension
        5.  The final value for each row is the line of text the value was parsed from
    """

    def __init__(self,
                 key_field: str,
                 source_data_name: str,
                 collection_date: str or None,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            2-Apr-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/45
        Updated:
            12-Nov-2019
            craig.trim@ibm.com
            *   dust-off and refactor in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331
        :param key_field:
        :param source_data_name:
        :param collection_date:
        :param mongo_client:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self._is_debug = is_debug
        self._key_field = key_field
        self._mongo_client = mongo_client
        self._collection_date = collection_date
        self._source_data_name = source_data_name

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate GenerateFeedback Report",
                f"\tKey Field: {self._key_field}",
                f"\tCollection Date: {self._collection_date}",
                f"\tSource Data Name: {self._source_data_name}"]))

    def _fpath(self,
               collection_date: str,
               key_field: str) -> str:
        """
        Purpose:
            Generate an Output File Name for the Feedback Report
        Sample Output:
            feedback-supply_xdm_20191111-1A5085897.xlsx
        Notes:
            -   The file extension MUST BE 'xlsx'
            -   Changing the extension to 'csv' will result in this error:
                pandas.core.config.OptionError: "No such keys(s): 'io.excel.csv.writer'"
                ValueError: No engine for filetype: 'csv'
        :return:
            a valid excel file path
        """
        tts = str(time.time()).split('.')[0]
        fname = f"feedback-CD{collection_date}-ID{key_field}-TS{tts}.xlsx"
        return os.path.join(os.environ['GTS_BASE'],
                            'resources/output/testing',
                            fname)

    @staticmethod
    def _load_collection_names(collection_date: str or None,
                               source_data_name: str):
        if not collection_date:
            return CendantCollectionRegistry().latest().by_name(source_data_name).all()
        return CendantCollectionRegistry().by_date(collection_date).by_name(source_data_name).all()

    def _narrow_inference(self,
                          xdm_schema: str,
                          sheet_name: str,
                          tag_record: dict,
                          writer: ExcelWriter) -> None:
        """
        Purpose:
            Perform very high-precision inference (smaller number of records)
        :param xdm_schema:
            the schema to use
            e.g., 'supply' or 'learning'
        :param sheet_name:
            the excel sheet to write to
        :param tag_record:
            the tag record
        :param writer:
            the instantiated excel writer
        """
        inference_writer = TagInferenceWriter(xdm_schema=xdm_schema,
                                              tag_record=tag_record,
                                              is_debug=self._is_debug,
                                              excel_worksheet=writer.sheets[sheet_name])

        inference_writer.process(add_tag_syns=False,
                                 add_tag_rels=True,
                                 add_rel_syns=False,
                                 inference_level=1,
                                 add_wiki_references=False)

    def _wide_inference(self,
                        xdm_schema: str,
                        sheet_name: str,
                        tag_record: dict,
                        writer: ExcelWriter) -> None:
        """
        Purpose:
            Perform very high-recall inference (large number of records)
        :param xdm_schema:
            the schema to use
            e.g., 'supply' or 'learning'
        :param sheet_name:
            the excel sheet to write to
        :param tag_record:
            the tag record
        :param writer:
            the instantiated excel writer
        """
        inference_writer = TagInferenceWriter(xdm_schema=xdm_schema,
                                              tag_record=tag_record,
                                              is_debug=self._is_debug,
                                              excel_worksheet=writer.sheets[sheet_name])

        inference_writer.process(add_tag_syns=True,
                                 add_tag_rels=True,
                                 add_rel_syns=True,
                                 inference_level=2,
                                 add_wiki_references=True)

    def _inference(self,
                   tag_record: dict,
                   writer: ExcelWriter) -> None:
        """
        Purpose:
            Write Inference Tags
        :param tag_record:
            the tag record
        :param writer:
            the instantiated excel writer
        """

        self._narrow_inference(writer=writer,
                               sheet_name='SI-1',
                               xdm_schema='supply',
                               tag_record=tag_record)

        self._wide_inference(writer=writer,
                             sheet_name='SI-2',
                             xdm_schema='supply',
                             tag_record=tag_record)

        self._narrow_inference(writer=writer,
                               sheet_name='LI-1',
                               xdm_schema='learning',
                               tag_record=tag_record)

        self._wide_inference(writer=writer,
                             sheet_name='LI-2',
                             xdm_schema='learning',
                             tag_record=tag_record)

    def process(self):
        """
          Processes the logs from the input directory
          @input: Base directory containing the input and output subdirs.
          @output: None
        """

        collection_names = self._load_collection_names(collection_date=self._collection_date,
                                                       source_data_name=self._source_data_name)

        records = RecordRetriever(is_debug=self._is_debug,
                                  key_field=self._key_field,
                                  mongo_client=self._mongo_client,
                                  collection_names=collection_names).process()

        if not records["tag"] and not records["xdm"]:
            raise ValueError("Data Not Found")

        fpath = self._fpath(key_field=records["tag"]["key_field"],
                            collection_date=collection_names["tag"].split("_")[-1].strip())

        writer = ExcelWriterInitializer(is_debug=self._is_debug,
                                        file_path=fpath,
                                        tabs=['XDM-Tag',
                                              'XDM-Weight',
                                              'Tags',
                                              'SI-1',
                                              'SI-2',
                                              'LI-1',
                                              'LI-2',
                                              'Academic',
                                              'Time']).process()

        if records["tag"]:
            TagReportWriter(tag_record=records["tag"],
                            is_debug=self._is_debug,
                            key_field=self._key_field,
                            excel_worksheet=writer.sheets['Tags']).process()

            self._inference(writer=writer,
                            tag_record=records["tag"])

            AcademicDimensionWriter(is_debug=self._is_debug,
                                    tag_record=records["tag"],
                                    excel_worksheet=writer.sheets['Academic']).process()

            TimeDimensionWriter(is_debug=self._is_debug,
                                tag_record=records["tag"],
                                excel_worksheet=writer.sheets['Time']).process()

            if records["xdm"]:
                DimensionTagWriter(tag_record=records["tag"],
                                   xdm_record=records["xdm"],
                                   is_debug=self._is_debug,
                                   key_field=self._key_field,
                                   excel_worksheet=writer.sheets['XDM-Tag']).process()

                DimensionWeightWriter(xdm_record=records["xdm"],
                                      xdm_schema=self._source_data_name,
                                      is_debug=self._is_debug,
                                      excel_worksheet=writer.sheets['XDM-Weight']).process()

        writer.save()


def main():
    GenerateFeedbackReport(key_field="1A5085897",
                           is_debug=False,
                           collection_date="latest",
                           source_data_name="supply").process()


if __name__ == "__main__":
    main()
